import polars as pl
from pathlib import Path
import pyreadstat
import datetime
import pandas as pd

# ============================================
# HELPER FUNCTIONS
# ============================================

def format_number(val):
    """Format number to display without scientific notation"""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return '.'
    if isinstance(val, float):
        if val.is_integer():
            return str(int(val))
        else:
            return f"{val:.2f}"
    return str(val)

def generate_text_report(float_only, output_path):
    """Generate text report matching production SAS output format exactly"""
    print("\n[STEP 10] Generating text report...")
    
    report_path = output_path / "EIFLTEXP_REPORT.txt"
    
    with open(report_path, 'w') as f:
        # Header matching SAS output
        f.write("The Python Processing System\n")
        f.write(f"{datetime.datetime.now().strftime('%H:%M %A, %B %d, %Y')}\n")
        f.write(" " * 50 + "1\n")
        f.write("\n")
        
        # Column headers - exactly matching SAS
        f.write("Obs PRODCD AMTIND BRANCH   ACCTNO   LEDGBAL PRODUCT CURBAL INTPAYBL _TYPE_ _FREQ_   FLOAT    AVBAL    AVBALTT  CURBALTT\n")
        f.write("                                                                                                                       \n")
        
        if not float_only.is_empty():
            float_only_sorted = float_only.sort('acctno')
            
            obs_count = 0
            total_float = 0
            
            for row in float_only_sorted.rows():
                obs_count += 1
                
                acctno = row[0] if len(row) > 0 else ''
                float_val = row[1] if len(row) > 1 else 0
                prodcd = row[2] if len(row) > 2 else ''
                amtind = row[3] if len(row) > 3 else ''
                branch = row[4] if len(row) > 4 else ''
                ledgbal = row[5] if len(row) > 5 else 0
                product = row[6] if len(row) > 6 else ''
                curbal = row[7] if len(row) > 7 else 0
                intpaybl = row[8] if len(row) > 8 else 0
                avbal = row[9] if len(row) > 9 else 0
                avbaltt = row[10] if len(row) > 10 else 0
                curbaltt = row[11] if len(row) > 11 else 0
                
                # Format values exactly like SAS output
                acctno_str = format_number(acctno)
                prodcd_str = format_number(prodcd) if prodcd and not pd.isna(prodcd) else '.'
                amtind_str = format_number(amtind) if amtind and not pd.isna(amtind) else '.'
                branch_str = format_number(branch) if branch and not pd.isna(branch) else '.'
                ledgbal_str = format_number(ledgbal) if ledgbal and not pd.isna(ledgbal) else '.'
                product_str = format_number(product) if product and not pd.isna(product) else '.'
                curbal_str = format_number(curbal) if curbal and not pd.isna(curbal) else '0'
                intpaybl_str = format_number(intpaybl) if intpaybl and not pd.isna(intpaybl) else '.'
                float_str = format_number(float_val) if float_val and not pd.isna(float_val) else '.'
                avbal_str = format_number(avbal) if avbal and not pd.isna(avbal) else '.'
                avbaltt_str = format_number(avbaltt) if avbaltt and not pd.isna(avbaltt) else '.'
                curbaltt_str = format_number(curbaltt) if curbaltt and not pd.isna(curbaltt) else '0'
                
                f.write(f"{obs_count:3} {prodcd_str:>6} {amtind_str:>6} {branch_str:>6} {acctno_str:>10} {ledgbal_str:>10} {product_str:>7} {curbal_str:>10} {intpaybl_str:>8} {1:>6} {1:>6} {float_str:>10} {avbal_str:>10} {avbaltt_str:>10} {curbaltt_str:>10}\n")
                
                total_float += float_val if float_val and not pd.isna(float_val) else 0
            
            f.write(" " * 50 + "========\n")
            f.write(f"{' ' * 87}{total_float:.2f}\n")
        else:
            f.write("No records found matching B AND NOT A condition.\n")
            f.write("All FLOAT records have matching DEPOSIT records.\n")
        
        f.write("\n")
    
    print(f"  - Report saved to {report_path}")
    print(f"  - Total FLOAT in report: {total_float if not float_only.is_empty() else 0:.2f}")

def read_sas_file(filepath, columns=None):
    """Read SAS file and return Polars DataFrame with selected columns"""
    try:
        if columns:
            try:
                df, meta = pyreadstat.read_sas7bdat(filepath, column_names=columns)
            except TypeError:
                try:
                    df, meta = pyreadstat.read_sas7bdat(filepath, columns=columns)
                except TypeError:
                    df, meta = pyreadstat.read_sas7bdat(filepath)
                    existing_cols = [col for col in columns if col in df.columns]
                    if existing_cols:
                        df = df[existing_cols]
        else:
            df, meta = pyreadstat.read_sas7bdat(filepath)
        
        return pl.DataFrame(df)
        
    except FileNotFoundError:
        print(f"  - WARNING: {filepath.name} not found")
        return None
    except Exception as e:
        print(f"  - ERROR reading {filepath.name}: {e}")
        return None

def standardize_columns(df):
    """Convert column names to lowercase"""
    if df.is_empty():
        return df
    df = df.rename({col: col.lower() for col in df.columns})
    return df

def standardize_dataframe(df, columns_to_keep):
    """Standardize DataFrame"""
    if df.is_empty():
        return df
    
    existing_cols = [col for col in columns_to_keep if col in df.columns]
    if existing_cols:
        df = df.select(existing_cols)
    
    for col in columns_to_keep:
        if col not in df.columns:
            df = df.with_columns([
                pl.lit(None).cast(pl.Float64 if col in ['acctno', 'curbal', 'ledgbal', 'intpaybl'] else pl.Utf8).alias(col)
            ])
    
    if 'acctno' in df.columns:
        df = df.with_columns([
            pl.col('acctno').cast(pl.Float64)
        ])
    
    for col in ['curbal', 'ledgbal', 'intpaybl']:
        if col in df.columns:
            try:
                df = df.with_columns([
                    pl.col(col).cast(pl.Float64)
                ])
            except:
                df = df.with_columns([
                    pl.col(col).cast(pl.Utf8).str.replace_all(',', '').cast(pl.Float64)
                ])
    
    return df

def standardize_schema_for_concat(dfs):
    """Standardize all DataFrames to have the same schema"""
    if not dfs:
        return dfs
    
    all_cols = set()
    for df in dfs:
        all_cols.update(df.columns)
    all_cols = sorted(all_cols)
    
    standardized_dfs = []
    for df in dfs:
        for col in all_cols:
            if col not in df.columns:
                if col == 'amtind':
                    df = df.with_columns([
                        pl.lit(None).cast(pl.Utf8).alias(col)
                    ])
                else:
                    df = df.with_columns([
                        pl.lit(None).cast(pl.Float64 if col in ['acctno', 'curbal', 'ledgbal', 'intpaybl'] else pl.Utf8).alias(col)
                    ])
        
        for col in ['acctno', 'curbal', 'ledgbal', 'intpaybl']:
            if col in df.columns:
                try:
                    df = df.with_columns([
                        pl.col(col).cast(pl.Float64)
                    ])
                except:
                    df = df.with_columns([
                        pl.col(col).cast(pl.Utf8).str.replace_all(',', '').cast(pl.Float64)
                    ])
        
        df = df.select(all_cols)
        standardized_dfs.append(df)
    
    return standardized_dfs

# ============================================
# MAIN PROCESSING
# ============================================

# Configuration
mni_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/MNI")
imni_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/IMNI")
pidms_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/PIDM")
output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFLTEXP")
output_path.mkdir(exist_ok=True)

print("="*60)
print("EIFLTEXP PROCESSING STARTED")
print("="*60)
print(f"MNI Path: {mni_path}")
print(f"IMNI Path: {imni_path}")
print(f"PIDM Path: {pidms_path}")
print(f"Output Path: {output_path}")
print("="*60)

STANDARD_COLUMNS = ['acctno', 'branch', 'curbal', 'ledgbal', 'amtind', 'intpaybl', 'product', 'progcd']

# ============================================
# STEP 1: Load and combine FDMTHLY (MNI + IMNI)
# ============================================
print("\n[STEP 1] Loading FDMTHLY data (MNI + IMNI)...")

fdmthly_dfs = []
for path, name in [(mni_path, "MNI"), (imni_path, "IMNI")]:
    df = read_sas_file(path / "fdmthly.sas7bdat", 
                      ['ACCTNO', 'BRANCH', 'INTPLAN', 'CURBAL', 'BIC', 'AMTIND', 'INTPAY'])
    if df is not None:
        df = standardize_columns(df)
        print(f"  - {name} FDMTHLY loaded: {df.height} records")
        fdmthly_dfs.append(df)
    else:
        print(f"  - {name} FDMTHLY not found")

if fdmthly_dfs:
    standardized_fdmthly = standardize_schema_for_concat(fdmthly_dfs)
    fdmthly_combined = pl.concat(standardized_fdmthly, how="diagonal")
    print(f"  - Combined FDMTHLY: {fdmthly_combined.height} records")
    
    fdmthly_combined = fdmthly_combined.rename({
        'bic': 'progcd',
        'intplan': 'product',
        'intpay': 'intpaybl'
    })
    
    fdmthly_combined = fdmthly_combined.with_columns([
        pl.col('curbal').alias('ledgbal')
    ])
    
    fdmthly_processed = standardize_dataframe(fdmthly_combined, STANDARD_COLUMNS)
    
    if 'acctno' in fdmthly_processed.columns:
        fdmthly_processed = fdmthly_processed.with_columns([
            pl.col('acctno').cast(pl.Float64)
        ])
    
    fdmthly_processed.write_parquet(output_path / "FDMTHLY.parquet")
    print(f"  - FDMTHLY saved")
else:
    fdmthly_processed = pl.DataFrame()
    print("  - No FDMTHLY data")

# ============================================
# STEP 2: Load and combine CURN (MNI + IMNI)
# ============================================
print("\n[STEP 2] Loading CURN data (MNI + IMNI)...")

curn_dfs = []
for path, name in [(mni_path, "MNI"), (imni_path, "IMNI")]:
    df = read_sas_file(path / "curn124.sas7bdat",
                      ['ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH'])
    if df is not None:
        df = standardize_columns(df)
        if 'amtind' in df.columns:
            df = df.with_columns([pl.col('amtind').cast(pl.Utf8)])
        print(f"  - {name} CURN124 loaded: {df.height} records")
        curn_dfs.append(df)
    else:
        print(f"  - {name} CURN124 not found")

if curn_dfs:
    standardized_curn = standardize_schema_for_concat(curn_dfs)
    curn_combined = pl.concat(standardized_curn, how="diagonal")
    print(f"  - Combined CURN: {curn_combined.height} records")
    
    if 'product' in curn_combined.columns:
        curn_filtered = curn_combined.filter(pl.col('product') != 139)
        print(f"  - CURN filtered (removed PRODUCT=139): {curn_filtered.height} records")
    else:
        curn_filtered = curn_combined
    
    if 'acctno' in curn_filtered.columns:
        curn_filtered = curn_filtered.with_columns([
            pl.col('acctno').cast(pl.Float64)
        ])
    
    if 'amtind' in curn_filtered.columns:
        curn_filtered = curn_filtered.with_columns([
            pl.col('amtind').cast(pl.Utf8)
        ])
else:
    curn_filtered = pl.DataFrame()
    print("  - No CURN data")

# ============================================
# STEP 3: Load SAVG (MNI + IMNI)
# ============================================
print("\n[STEP 3] Loading SAVG data (MNI + IMNI)...")

datasets_to_combine = []
dataset_names = []

for path, name in [(mni_path, "MNI"), (imni_path, "IMNI")]:
    df = read_sas_file(path / "savg124.sas7bdat",
                      ['ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH'])
    if df is not None:
        df = standardize_columns(df)
        if 'amtind' in df.columns:
            df = df.with_columns([pl.col('amtind').cast(pl.Utf8)])
        print(f"  - {name} SAVG124 loaded: {df.height} records")
        
        if 'acctno' in df.columns:
            df = df.with_columns([pl.col('acctno').cast(pl.Float64)])
        
        df_standardized = standardize_dataframe(df, STANDARD_COLUMNS)
        datasets_to_combine.append(df_standardized)
        dataset_names.append(f"{name} SAVG124")
    else:
        print(f"  - {name} SAVG124 not loaded")

# ============================================
# STEP 4: Add CURN
# ============================================
print("\n[STEP 4] Adding CURN to dataset list...")

if not curn_filtered.is_empty():
    curn_standardized = standardize_dataframe(curn_filtered, STANDARD_COLUMNS)
    datasets_to_combine.append(curn_standardized)
    dataset_names.append("CURN")
    print(f"  - CURN added with {curn_standardized.height} records")
else:
    print("  - No CURN data to add")

# ============================================
# STEP 5: Add FDMTHLY
# ============================================
print("\n[STEP 5] Adding FDMTHLY to dataset list...")

if not fdmthly_processed.is_empty():
    datasets_to_combine.append(fdmthly_processed)
    dataset_names.append("FDMTHLY")
    print(f"  - FDMTHLY added with {fdmthly_processed.height} records")
else:
    print("  - No FDMTHLY data to add")

# ============================================
# STEP 6: Combine all datasets
# ============================================
print("\n[STEP 6] Combining all datasets...")

print(f"  - Total datasets to combine: {len(datasets_to_combine)}")
for i, name in enumerate(dataset_names):
    print(f"    {i+1}. {name}: {datasets_to_combine[i].height} records")

if datasets_to_combine:
    standardized_all = standardize_schema_for_concat(datasets_to_combine)
    deposit_combined = pl.concat(standardized_all, how="diagonal")
    print(f"  - Combined data: {deposit_combined.height} records")
    
    # DEBUG: Check what's in prodcd column
    print("\n  - DEBUG: Sample of prodcd values:")
    if 'progcd' in deposit_combined.columns:
        sample_progcd = deposit_combined.select('progcd').head(20).to_list()
        print(f"    {sample_progcd}")
        print(f"    Unique prodcd values: {deposit_combined['progcd'].n_unique()}")
        print(f"    Null prodcd count: {deposit_combined['progcd'].is_null().sum()}")
    
    # ============================================
    # STEP 7: Apply filters (matching SAS exactly)
    # ============================================
    print("\n[STEP 7] Applying filters and transformations...")
    
    if 'progcd' not in deposit_combined.columns:
        print("  - ERROR: 'progcd' column not found")
        deposit_filtered = pl.DataFrame()
    else:
        # Debug: Count before filter
        print(f"  - Before PROGCD filter: {deposit_combined.height}")
        
        valid_progcd = [
            '42110', '42310', '42120', '42320', '42130', '42610',
            '42133', '42132', '42180', '42610', '42630', '34180',
            '42199', '42699'
        ]
        
        # Filter by PROGCD
        deposit_filtered = deposit_combined.filter(
            pl.col('progcd').is_in(valid_progcd)
        )
        print(f"  - After PROGCD filter: {deposit_filtered.height} records")
        print(f"  - DEBUG: Filtered prodcd values: {deposit_filtered['progcd'].unique().to_list()[:10]}")
        
        if 'product' in deposit_filtered.columns and not deposit_filtered.is_empty():
            # Convert product to appropriate type if needed
            if deposit_filtered['product'].dtype != pl.Float64:
                deposit_filtered = deposit_filtered.with_columns([
                    pl.col('product').cast(pl.Float64)
                ])
            
            # IF PRODUCT = 166 THEN PRODCD = '42310'
            deposit_filtered = deposit_filtered.with_columns([
                pl.when(pl.col('product') == 166)
                .then(pl.lit('42310'))
                .otherwise(pl.col('progcd'))
                .alias('progcd')
            ])
            print(f"  - After PRODUCT=166: {deposit_filtered.height} records")
            
            # IF PRODCD IN ('42199','42699') AND PRODUCT NOT IN (72,413) THEN DELETE
            deposit_filtered = deposit_filtered.filter(
                ~(
                    pl.col('progcd').is_in(['42199', '42699']) & 
                    ~pl.col('product').is_in([72, 413])
                )
            )
            print(f"  - After PROGCD special: {deposit_filtered.height} records")
            
            # IF PRODUCT IN (30,31,32,33,34) THEN DELETE
            deposit_filtered = deposit_filtered.filter(
                ~pl.col('product').is_in([30, 31, 32, 33, 34])
            )
            print(f"  - After PRODUCT filter: {deposit_filtered.height} records")
        
        if 'intpaybl' in deposit_filtered.columns and not deposit_filtered.is_empty():
            if deposit_filtered['intpaybl'].dtype != pl.Float64:
                deposit_filtered = deposit_filtered.with_columns([
                    pl.col('intpaybl').cast(pl.Float64)
                ])
            
            deposit_filtered = deposit_filtered.with_columns([
                pl.when(pl.col('intpaybl') < 0)
                .then(0)
                .otherwise(pl.col('intpaybl'))
                .alias('intpaybl')
            ])
            print(f"  - After INTPAYBL: {deposit_filtered.height} records")
        
        if not deposit_filtered.is_empty():
            deposit_filtered.write_parquet(output_path / "DEPOSIT.parquet")
            print(f"  - DEPOSIT saved with {deposit_filtered.height} records")
            
            # DEBUG: Show sample of DEPOSIT
            print("\n  - DEBUG: Sample DEPOSIT records (first 5):")
            print(deposit_filtered.head(5))
        else:
            print("  - DEPOSIT is empty after all filters")
else:
    deposit_filtered = pl.DataFrame()
    print("  - No datasets to combine")

# ============================================
# STEP 8: Load FLOAT
# ============================================
print("\n[STEP 8] Loading FLOAT data...")

float_df = read_sas_file(pidms_path / "float.sas7bdat")
if float_df is not None:
    float_df = standardize_columns(float_df)
    print(f"  - FLOAT loaded: {float_df.height} records")
    
    if 'float' in float_df.columns:
        float_df = float_df.with_columns([pl.col('float').cast(pl.Float64)])
    
    if 'acctno' in float_df.columns:
        float_df = float_df.with_columns([pl.col('acctno').cast(pl.Float64)])
    
    float_df.write_parquet(output_path / "FLOAT.parquet")
    
    if not float_df.is_empty() and 'float' in float_df.columns and 'acctno' in float_df.columns:
        # Aggregate FLOAT by ACCTNO (sum) - matching PROC SUMMARY
        float_summary = float_df.group_by('acctno').agg([
            pl.col('float').sum().alias('float')
        ])
        float_summary.write_parquet(output_path / "FLOAT_SUMMARY.parquet")
        print(f"  - FLOAT_SUMMARY saved: {float_summary.height} records")
        
        # DEBUG: Show sample FLOAT
        print("\n  - DEBUG: Sample FLOAT records (first 5):")
        print(float_summary.head(5))
    else:
        float_summary = pl.DataFrame()
else:
    float_df = pl.DataFrame()
    float_summary = pl.DataFrame()

# ============================================
# STEP 9: Merge DEPOSIT and FLOAT, apply B AND NOT A
# ============================================
print("\n[STEP 9] Merging DEPOSIT and FLOAT, applying B AND NOT A...")

if not deposit_filtered.is_empty() and not float_summary.is_empty():
    # Sort DEPOSIT by ACCTNO (matching PROC SORT)
    deposit_sorted = deposit_filtered.sort('acctno')
    
    # DEBUG: Check ACCTNO overlap
    deposit_acctnos = set(deposit_filtered['acctno'].unique().to_list())
    float_acctnos = set(float_summary['acctno'].unique().to_list())
    
    print(f"  - DEPOSIT unique ACCTNO count: {len(deposit_acctnos)}")
    print(f"  - FLOAT unique ACCTNO count: {len(float_acctnos)}")
    
    overlap = deposit_acctnos.intersection(float_acctnos)
    print(f"  - Overlap ACCTNO count: {len(overlap)}")
    print(f"  - FLOAT records without DEPOSIT: {len(float_acctnos - deposit_acctnos)}")
    
    if len(float_acctnos - deposit_acctnos) > 0:
        print("\n  - DEBUG: FLOAT ACCTNO without DEPOSIT (first 10):")
        for acct in list(float_acctnos - deposit_acctnos)[:10]:
            print(f"    {int(acct)}")
    
    # Merge DEPOSIT(IN=A) FLOAT(IN=B) - matching SAS MERGE
    merged = deposit_sorted.join(
        float_summary, 
        on='acctno', 
        how='full',  # Equivalent to SAS MERGE with IN= options
        suffix='_float'
    )
    
    print(f"  - Merged records: {merged.height}")
    
    # Apply transformations matching SAS
    # IF CURBAL < 0 THEN CURBAL = 0;
    if 'curbal' in merged.columns:
        if merged['curbal'].dtype != pl.Float64:
            merged = merged.with_columns([
                pl.col('curbal').cast(pl.Float64)
            ])
        merged = merged.with_columns([
            pl.when(pl.col('curbal') < 0).then(0).otherwise(pl.col('curbal')).alias('curbal')
        ])
    
    # AVBAL = SUM(CURBAL,(-1)*FLOAT);
    if 'curbal' in merged.columns and 'float' in merged.columns:
        if merged['float'].dtype != pl.Float64:
            merged = merged.with_columns([
                pl.col('float').cast(pl.Float64)
            ])
        merged = merged.with_columns([
            (pl.col('curbal') - pl.col('float')).alias('avbal')
        ])
    
    # AVBALTT = SUM(AVBAL,INTPAYBL);
    if 'avbal' in merged.columns and 'intpaybl' in merged.columns:
        if merged['intpaybl'].dtype != pl.Float64:
            merged = merged.with_columns([
                pl.col('intpaybl').cast(pl.Float64)
            ])
        merged = merged.with_columns([
            (pl.col('avbal') + pl.col('intpaybl')).alias('avbaltt')
        ])
    
    # CURBALTT = SUM(CURBAL,INTPAYBL);
    if 'curbal' in merged.columns and 'intpaybl' in merged.columns:
        merged = merged.with_columns([
            (pl.col('curbal') + pl.col('intpaybl')).alias('curbaltt')
        ])
    
    # IF B AND NOT A - Keep only FLOAT records without matching DEPOSIT
    float_only = merged.filter(
        pl.col('float').is_not_null() & 
        ~pl.col('acctno').is_in(list(deposit_acctnos))
    )
    
    print(f"  - FLOAT records not in DEPOSIT (B AND NOT A): {float_only.height}")
    
    if not float_only.is_empty():
        # Create output with proper columns matching SAS
        float_only_output = float_only.with_columns([
            pl.lit(None).cast(pl.Utf8).alias('progcd'),
            pl.lit(None).cast(pl.Utf8).alias('amtind'),
            pl.lit(None).cast(pl.Float64).alias('branch'),
            pl.lit(None).cast(pl.Float64).alias('ledgbal'),
            pl.lit(None).cast(pl.Float64).alias('product'),
            pl.lit(0).cast(pl.Float64).alias('curbal'),
            pl.lit(None).cast(pl.Float64).alias('intpaybl'),
            pl.col('float').alias('avbal'),
            (-pl.col('float')).alias('avbaltt'),
            pl.lit(0).cast(pl.Float64).alias('curbaltt')
        ]).select([
            'acctno', 'float', 'progcd', 'amtind', 'branch', 
            'ledgbal', 'product', 'curbal', 'intpaybl',
            'avbal', 'avbaltt', 'curbaltt'
        ])
        
        # Generate text report
        generate_text_report(float_only_output, output_path)
        
        # Save FLOAT_ONLY records
        float_only_output.write_parquet(output_path / "FLOAT_ONLY.parquet")
        float_only_output.write_csv(output_path / "FLOAT_ONLY.csv")
        print(f"\n  - FLOAT_ONLY saved: {float_only.height} records")
        print(f"  - Total FLOAT amount in FLOAT_ONLY: {float_only['float'].sum():.2f}")
        
        print("\n" + "="*60)
        print("B AND NOT A SUMMARY")
        print("="*60)
        print(f"Total FLOAT_ONLY records: {float_only.height}")
        print(f"Total FLOAT amount: {float_only['float'].sum():.2f}")
        print("="*60)
        
    else:
        print("\n  No FLOAT_ONLY records found")
        float_only_output = pl.DataFrame()
        generate_text_report(float_only_output, output_path)
        
else:
    print("  - No DEPOSIT or FLOAT data to process")
    float_only_output = pl.DataFrame()
    generate_text_report(float_only_output, output_path)

print("\n" + "="*60)
print("PROCESSING COMPLETED SUCCESSFULLY")
print("="*60)
