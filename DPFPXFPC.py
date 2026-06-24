import polars as pl
import duckdb
from pathlib import Path
import pyreadstat
import datetime

# ============================================
# HELPER FUNCTIONS (Define before use)
# ============================================

def generate_text_report(deposit_merged, float_only, output_path):
    """Generate text report from merged data"""
    print("\n[STEP 10] Generating text report...")
    
    report_path = output_path / "EIFLTEXP_REPORT.txt"
    
    with open(report_path, 'w') as f:
        f.write("="*70 + "\n")
        f.write("EIFLTEXP PROCESSING REPORT\n")
        f.write(f"Generated: {datetime.datetime.now()}\n")
        f.write("="*70 + "\n\n")
        
        # Summary statistics
        f.write("SUMMARY STATISTICS\n")
        f.write("-"*70 + "\n")
        
        if not deposit_merged.is_empty():
            total_float = deposit_merged.select(pl.col('float').sum()).row(0)[0]
            total_avbal = deposit_merged.select(pl.col('avbal').sum()).row(0)[0]
            total_curbal = deposit_merged.select(pl.col('curbal').sum()).row(0)[0]
            
            f.write(f"Total Records: {deposit_merged.height:,}\n")
            f.write(f"Total CURBAL: {total_curbal:,.2f}\n")
            f.write(f"Total FLOAT: {total_float:,.2f}\n")
            f.write(f"Total AVBAL: {total_avbal:,.2f}\n\n")
        
        # Float only records
        f.write("FLOAT ONLY RECORDS (In FLOAT but not in DEPOSIT)\n")
        f.write("-"*70 + "\n")
        f.write(f"Total Records: {float_only.height:,}\n")
        
        if not float_only.is_empty():
            f.write("\nTop 10 ACCTNO by FLOAT Amount:\n")
            float_only_top = float_only.sort('float', descending=True).select(['acctno', 'float']).head(10)
            for row in float_only_top.rows():
                f.write(f"  ACCTNO: {row[0]}, FLOAT: {row[1]:,.2f}\n")
        
        f.write("\n" + "="*70 + "\n")
        f.write("END OF REPORT\n")
    
    print(f"  - Report saved to {report_path}")

def read_sas_file(filepath, columns=None):
    """Read SAS file and return Polars DataFrame with selected columns"""
    try:
        if columns:
            # Try different parameter names for different pyreadstat versions
            try:
                # Try with column_names parameter (newer versions)
                df, meta = pyreadstat.read_sas7bdat(filepath, column_names=columns)
            except TypeError:
                # Try with columns parameter (older versions)
                try:
                    df, meta = pyreadstat.read_sas7bdat(filepath, columns=columns)
                except TypeError:
                    # Read all columns then select
                    df, meta = pyreadstat.read_sas7bdat(filepath)
                    # Filter to only columns that exist
                    existing_cols = [col for col in columns if col in df.columns]
                    if existing_cols:
                        df = df[existing_cols]
        else:
            df, meta = pyreadstat.read_sas7bdat(filepath)
        
        # Convert to Polars DataFrame
        return pl.DataFrame(df)
        
    except FileNotFoundError:
        print(f"  - WARNING: {filepath.name} not found")
        return None
    except Exception as e:
        print(f"  - ERROR reading {filepath.name}: {e}")
        return None

def standardize_columns(df):
    """Convert column names to lowercase and handle common variations"""
    if df.is_empty():
        return df
    
    # Convert all column names to lowercase
    df = df.rename({col: col.lower() for col in df.columns})
    return df

def standardize_dataframe(df, columns_to_keep):
    """Standardize DataFrame: keep specified columns and ensure consistent data types"""
    if df.is_empty():
        return df
    
    # Keep only the columns we want
    existing_cols = [col for col in columns_to_keep if col in df.columns]
    if existing_cols:
        df = df.select(existing_cols)
    
    # Add any missing columns with null values
    for col in columns_to_keep:
        if col not in df.columns:
            df = df.with_columns([
                pl.lit(None).cast(pl.Float64 if col == 'acctno' else pl.Utf8).alias(col)
            ])
    
    # Standardize acctno to Float64 for consistency
    if 'acctno' in df.columns:
        df = df.with_columns([
            pl.col('acctno').cast(pl.Float64)
        ])
    
    # Standardize curbal, ledgbal, amtind, intpaybl to Float64
    for col in ['curbal', 'ledgbal', 'amtind', 'intpaybl']:
        if col in df.columns:
            # Try to cast to Float64, if it fails, keep as is
            try:
                df = df.with_columns([
                    pl.col(col).cast(pl.Float64)
                ])
            except:
                # If casting fails, convert to string first then float
                df = df.with_columns([
                    pl.col(col).cast(pl.Utf8).str.replace_all(',', '').cast(pl.Float64)
                ])
    
    return df

def standardize_schema_for_concat(dfs):
    """Standardize all DataFrames to have the same schema for concatenation"""
    if not dfs:
        return dfs
    
    # Get all columns from all dataframes
    all_cols = set()
    for df in dfs:
        all_cols.update(df.columns)
    all_cols = sorted(all_cols)
    
    # Create a standardized version of each DataFrame
    standardized_dfs = []
    for df in dfs:
        # Add missing columns
        for col in all_cols:
            if col not in df.columns:
                df = df.with_columns([
                    pl.lit(None).cast(pl.Float64 if col in ['acctno', 'curbal', 'ledgbal', 'amtind', 'intpaybl'] else pl.Utf8).alias(col)
                ])
        
        # Ensure numeric columns are Float64
        for col in ['acctno', 'curbal', 'ledgbal', 'amtind', 'intpaybl']:
            if col in df.columns:
                try:
                    df = df.with_columns([
                        pl.col(col).cast(pl.Float64)
                    ])
                except:
                    # If it's a string, try to clean and convert
                    df = df.with_columns([
                        pl.col(col).cast(pl.Utf8).str.replace_all(',', '').cast(pl.Float64)
                    ])
        
        # Reorder columns
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

# Define standard columns to keep across all datasets (based on SAS code)
# FDMTHLY: ACCTNO BRANCH INTPLAN CURBAL BIC AMTIND INTPAY
# Rename: BIC=PRODCD, INTPLAN=PRODUCT, INTPAY=INTPAYBL
STANDARD_COLUMNS = ['acctno', 'branch', 'curbal', 'ledgbal', 'amtind', 'intpaybl', 'product', 'progcd']

# ============================================
# STEP 1: Load FDMTHLY from MNI and IMNI
# ============================================
print("\n[STEP 1] Loading FDMTHLY data...")

# Read FDMTHLY with specific columns (lowercase filenames)
fdmthly_df = read_sas_file(mni_path / "fdmthly.sas7bdat", 
                          ['ACCTNO', 'BRANCH', 'INTPLAN', 'CURBAL', 'BIC', 'AMTIND', 'INTPAY'])
if fdmthly_df is not None:
    fdmthly_df = standardize_columns(fdmthly_df)
    print(f"  - MNI FDMTHLY columns: {fdmthly_df.columns}")
    print(f"  - MNI FDMTHLY loaded: {fdmthly_df.height} records")
    print(f"  - MNI FDMTHLY dtypes: {fdmthly_df.dtypes}")
else:
    fdmthly_df = pl.DataFrame()

ifdmthly_df = read_sas_file(imni_path / "fdmthly.sas7bdat",
                           ['ACCTNO', 'BRANCH', 'INTPLAN', 'CURBAL', 'BIC', 'AMTIND', 'INTPAY'])
if ifdmthly_df is not None:
    ifdmthly_df = standardize_columns(ifdmthly_df)
    print(f"  - IMNI FDMTHLY loaded: {ifdmthly_df.height} records")
    print(f"  - IMNI FDMTHLY dtypes: {ifdmthly_df.dtypes}")
else:
    ifdmthly_df = pl.DataFrame()

# Standardize both FDMTHLY dataframes before concatenating
fdmthly_dfs = [fdmthly_df, ifdmthly_df]
fdmthly_dfs = [df for df in fdmthly_dfs if not df.is_empty()]

if fdmthly_dfs:
    # Standardize schema for concatenation
    standardized_fdmthly = standardize_schema_for_concat(fdmthly_dfs)
    fdmthly_combined = pl.concat(standardized_fdmthly, how="diagonal")
    print(f"  - Combined FDMTHLY: {fdmthly_combined.height} records")
    
    # Rename columns according to SAS logic: BIC=PRODCD, INTPLAN=PRODUCT, INTPAY=INTPAYBL
    fdmthly_combined = fdmthly_combined.rename({
        'bic': 'progcd',
        'intplan': 'product',
        'intpay': 'intpaybl'
    })
    
    # Add LEDGBAL = CURBAL
    fdmthly_combined = fdmthly_combined.with_columns([
        pl.col('curbal').alias('ledgbal')
    ])
    
    # Ensure all columns are in STANDARD_COLUMNS
    fdmthly_processed = standardize_dataframe(fdmthly_combined, STANDARD_COLUMNS)
    
    # Ensure acctno is Float64
    if 'acctno' in fdmthly_processed.columns:
        fdmthly_processed = fdmthly_processed.with_columns([
            pl.col('acctno').cast(pl.Float64)
        ])
    
    fdmthly_processed.write_parquet(output_path / "FDMTHLY.parquet")
    print(f"  - FDMTHLY saved to {output_path / 'FDMTHLY.parquet'}")
    print(f"  - FDMTHLY columns: {fdmthly_processed.columns}")
else:
    fdmthly_processed = pl.DataFrame()
    print("  - No FDMTHLY data to save")

# ============================================
# STEP 2: Load CURN from MNI and IMNI
# ============================================
print("\n[STEP 2] Loading CURN data...")

curn1_df = read_sas_file(mni_path / "curn124.sas7bdat",
                        ['ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH'])
if curn1_df is not None:
    curn1_df = standardize_columns(curn1_df)
    print(f"  - MNI CURN124 loaded: {curn1_df.height} records")
else:
    curn1_df = pl.DataFrame()

curn2_df = read_sas_file(imni_path / "curn124.sas7bdat",
                        ['ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH'])
if curn2_df is not None:
    curn2_df = standardize_columns(curn2_df)
    print(f"  - IMNI CURN124 loaded: {curn2_df.height} records")
else:
    curn2_df = pl.DataFrame()

# Standardize both CURN dataframes before concatenating
curn_dfs = [curn1_df, curn2_df]
curn_dfs = [df for df in curn_dfs if not df.is_empty()]

if curn_dfs:
    standardized_curn = standardize_schema_for_concat(curn_dfs)
    curn_combined = pl.concat(standardized_curn, how="diagonal")
    print(f"  - Combined CURN: {curn_combined.height} records")
    
    # IF PRODUCT = 139 THEN DELETE;
    if 'product' in curn_combined.columns:
        curn_filtered = curn_combined.filter(pl.col('product') != 139)
        print(f"  - CURN filtered (removed PRODUCT=139): {curn_filtered.height} records")
    else:
        curn_filtered = curn_combined
        print("  - WARNING: 'product' column not found in CURN data")
    
    # Ensure acctno is Float64
    if 'acctno' in curn_filtered.columns:
        curn_filtered = curn_filtered.with_columns([
            pl.col('acctno').cast(pl.Float64)
        ])
else:
    curn_filtered = pl.DataFrame()
    print("  - No CURN data")

# ============================================
# STEP 3: Load SAVG data
# ============================================
print("\n[STEP 3] Loading SAVG data...")

datasets_to_combine = []
dataset_names = []

# MNI.SAVG124
savg1_df = read_sas_file(mni_path / "savg124.sas7bdat",
                        ['ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH'])
if savg1_df is not None:
    savg1_df = standardize_columns(savg1_df)
    print(f"  - MNI SAVG124 loaded: {savg1_df.height} records")
    
    # Ensure acctno is Float64
    if 'acctno' in savg1_df.columns:
        savg1_df = savg1_df.with_columns([
            pl.col('acctno').cast(pl.Float64)
        ])
    
    savg1_standardized = standardize_dataframe(savg1_df, STANDARD_COLUMNS)
    datasets_to_combine.append(savg1_standardized)
    dataset_names.append("MNI SAVG124")
else:
    print("  - MNI SAVG124 not loaded")

# IMNI.SAVG124
savg2_df = read_sas_file(imni_path / "savg124.sas7bdat",
                        ['ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH'])
if savg2_df is not None:
    savg2_df = standardize_columns(savg2_df)
    print(f"  - IMNI SAVG124 loaded: {savg2_df.height} records")
    
    # Ensure acctno is Float64
    if 'acctno' in savg2_df.columns:
        savg2_df = savg2_df.with_columns([
            pl.col('acctno').cast(pl.Float64)
        ])
    
    savg2_standardized = standardize_dataframe(savg2_df, STANDARD_COLUMNS)
    datasets_to_combine.append(savg2_standardized)
    dataset_names.append("IMNI SAVG124")
else:
    print("  - IMNI SAVG124 not loaded")

# ============================================
# STEP 4: Add CURN to datasets
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
# STEP 5: Add FDMTHLY to datasets
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
    # Standardize all datasets before combining
    standardized_all = standardize_schema_for_concat(datasets_to_combine)
    deposit_combined = pl.concat(standardized_all, how="diagonal")
    print(f"  - Combined data: {deposit_combined.height} records")
    
    # ============================================
    # STEP 7: Apply filters and transformations
    # ============================================
    print("\n[STEP 7] Applying filters and transformations...")
    
    # Check if required columns exist
    if 'progcd' not in deposit_combined.columns:
        print("  - ERROR: 'progcd' column not found in combined data")
        deposit_filtered = pl.DataFrame()
    else:
        valid_progcd = [
            '42110', '42310', '42120', '42320', '42130', '42610',
            '42133', '42132', '42180', '42610', '42630', '34180',
            '42199', '42699'
        ]
        
        # Apply PROGCD filter
        deposit_filtered = deposit_combined.filter(
            pl.col('progcd').is_in(valid_progcd)
        )
        print(f"  - After PROGCD filter: {deposit_filtered.height} records")
        
        # IF PRODUCT = 166 THEN PROGCD = '42310';
        if 'product' in deposit_filtered.columns and not deposit_filtered.is_empty():
            deposit_filtered = deposit_filtered.with_columns([
                pl.when(pl.col('product') == 166)
                .then(pl.lit('42310'))
                .otherwise(pl.col('progcd'))
                .alias('progcd')
            ])
            print(f"  - After PRODUCT=166 transformation: {deposit_filtered.height} records")
            
            # IF PROGCD IN ('42199','42699') AND PRODUCT NOT IN (72,413) THEN DELETE;
            deposit_filtered = deposit_filtered.filter(
                ~(
                    pl.col('progcd').is_in(['42199', '42699']) & 
                    ~pl.col('product').is_in([72, 413])
                )
            )
            print(f"  - After PROGCD special filter: {deposit_filtered.height} records")
            
            # IF PRODUCT IN (30,31,32,33,34) THEN DELETE;
            deposit_filtered = deposit_filtered.filter(
                ~pl.col('product').is_in([30, 31, 32, 33, 34])
            )
            print(f"  - After PRODUCT filter: {deposit_filtered.height} records")
        
        # IF INTPAYBL < 0 THEN INTPAYBL = 0;
        if 'intpaybl' in deposit_filtered.columns and not deposit_filtered.is_empty():
            deposit_filtered = deposit_filtered.with_columns([
                pl.when(pl.col('intpaybl') < 0)
                .then(0)
                .otherwise(pl.col('intpaybl'))
                .alias('intpaybl')
            ])
            print(f"  - After INTPAYBL adjustment: {deposit_filtered.height} records")
        
        if not deposit_filtered.is_empty():
            deposit_filtered.write_parquet(output_path / "DEPOSIT.parquet")
            print(f"  - DEPOSIT saved to {output_path / 'DEPOSIT.parquet'}")
        else:
            print("  - DEPOSIT is empty after all filters")
else:
    deposit_filtered = pl.DataFrame()
    print("  - No datasets to combine")

# ============================================
# STEP 8: Load FLOAT data
# ============================================
print("\n[STEP 8] Loading FLOAT data...")

float_df = read_sas_file(pidms_path / "float.sas7bdat")
if float_df is not None:
    float_df = standardize_columns(float_df)
    print(f"  - FLOAT loaded: {float_df.height} records")
    float_df.write_parquet(output_path / "FLOAT.parquet")
    
    # PROC SUMMARY DATA=FLOAT NWAY; CLASS ACCTNO; VAR FLOAT; OUTPUT OUT=FLOAT SUM=;
    if not float_df.is_empty() and 'float' in float_df.columns and 'acctno' in float_df.columns:
        float_summary = float_df.with_columns([
            pl.col('acctno').cast(pl.Float64)
        ]).group_by('acctno').agg([
            pl.col('float').sum().alias('float')
        ])
        float_summary.write_parquet(output_path / "FLOAT_SUMMARY.parquet")
        print(f"  - FLOAT_SUMMARY saved: {float_summary.height} records")
    else:
        float_summary = pl.DataFrame()
else:
    float_df = pl.DataFrame()
    float_summary = pl.DataFrame()

# ============================================
# STEP 9: Merge DEPOSIT with FLOAT
# ============================================
print("\n[STEP 9] Merging DEPOSIT with FLOAT...")

if not deposit_filtered.is_empty():
    deposit_sorted = deposit_filtered.sort('acctno')
    
    if not float_summary.is_empty():
        # Ensure acctno is Float64
        deposit_sorted = deposit_sorted.with_columns([
            pl.col('acctno').cast(pl.Float64)
        ])
        float_summary = float_summary.with_columns([
            pl.col('acctno').cast(pl.Float64)
        ])
        
        deposit_merged = deposit_sorted.join(
            float_summary, on='acctno', how='left', suffix='_float'
        )
        print(f"  - Merge completed: {deposit_merged.height} records")
        
        # IF CURBAL < 0 THEN CURBAL = 0;
        deposit_merged = deposit_merged.with_columns([
            pl.when(pl.col('curbal') < 0)
            .then(0)
            .otherwise(pl.col('curbal'))
            .alias('curbal')
        ])
        
        # AVBAL = SUM(CURBAL,(-1)*FLOAT);
        deposit_merged = deposit_merged.with_columns([
            (pl.col('curbal') + (-1) * pl.col('float')).alias('avbal')
        ])
        
        # AVBALTT = SUM(AVBAL,INTPAYBL);
        # CURBALTT = SUM(CURBAL,INTPAYBL);
        deposit_merged = deposit_merged.with_columns([
            (pl.col('avbal') + pl.col('intpaybl')).alias('avbaltt'),
            (pl.col('curbal') + pl.col('intpaybl')).alias('curbaltt')
        ])
        
        # IF B AND NOT A; (keep records that are in FLOAT but not in DEPOSIT)
        float_only = deposit_merged.filter(
            pl.col('float').is_not_null() & 
            (pl.col('curbal').is_null() | pl.col('product').is_null())
        )
        
        deposit_merged.write_parquet(output_path / "DEPOSIT_MERGED.parquet")
        float_only.write_parquet(output_path / "FLOAT_ONLY.parquet")
        
        print(f"  - DEPOSIT_MERGED saved: {deposit_merged.height} records")
        print(f"  - FLOAT_ONLY saved: {float_only.height} records")
        
        # Generate text report
        generate_text_report(deposit_merged, float_only, output_path)
        
        # PROC PRINT DATA=DEPOSIT; SUM FLOAT;
        print("\n" + "="*60)
        print("DEPOSIT DATA WITH FLOAT SUMMARY")
        print("="*60)
        if not deposit_merged.is_empty():
            total_float = deposit_merged.select(pl.col('float').sum()).row(0)[0]
            total_avbal = deposit_merged.select(pl.col('avbal').sum()).row(0)[0]
            print(f"Total FLOAT: {total_float:,.2f}")
            print(f"Total AVBAL: {total_avbal:,.2f}")
            print(f"Total Records: {deposit_merged.height}")
        
        print("\nFLOAT ONLY RECORDS (B AND NOT A):")
        print(f"  - Records: {float_only.height}")
        if not float_only.is_empty():
            print(float_only.select(['acctno', 'float']).head(10))
        
    else:
        print("  - No FLOAT data found for merging")
        deposit_merged = deposit_sorted
        
else:
    print("  - No DEPOSIT data found for processing")

print("\n" + "="*60)
print("PROCESSING COMPLETED SUCCESSFULLY")
print("="*60)
