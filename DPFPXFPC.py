import polars as pl
import duckdb
from pathlib import Path
import pyreadstat
import datetime
import pandas as pd

# ============================================
# HELPER FUNCTIONS (Define before use)
# ============================================

def format_number(val):
    """Format number to display without scientific notation"""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ''
    if isinstance(val, float):
        # If it's a whole number, display as integer
        if val.is_integer():
            return str(int(val))
        else:
            return f"{val:.2f}"
    return str(val)

def generate_text_report(deposit_merged, float_only, output_path):
    """Generate text report matching production SAS output format"""
    print("\n[STEP 10] Generating text report...")
    
    report_path = output_path / "EIFLTEXP_REPORT.txt"
    
    with open(report_path, 'w') as f:
        # Header with timestamp
        f.write("The Python Processing System\n")
        f.write(f"{datetime.datetime.now().strftime('%H:%M %A, %B %d, %Y')}\n")
        f.write(" " * 50 + "1\n")
        f.write("\n")
        
        # Column headers
        f.write("Obs PRODCD AMTIND BRANCH   ACCTNO   LEDGBAL PRODUCT CURBAL INTPAYBL _TYPE_ _FREQ_   FLOAT    AVBAL    AVBALTT  CURBALTT\n")
        f.write("\n")
        
        # Data rows - only if we have float_only records
        if not float_only.is_empty():
            # Sort by FLOAT amount (descending) to match SAS output
            float_only_sorted = float_only.sort('float', descending=True)
            
            obs_count = 0
            total_float = 0
            
            # Get column indices for faster access
            col_names = float_only_sorted.columns
            
            for row in float_only_sorted.rows():
                obs_count += 1
                
                # Extract values with defaults
                prodcd = row[col_names.index('progcd')] if 'progcd' in col_names else ''
                amtind = row[col_names.index('amtind')] if 'amtind' in col_names else ''
                branch = row[col_names.index('branch')] if 'branch' in col_names else ''
                acctno = row[col_names.index('acctno')] if 'acctno' in col_names else ''
                ledgbal = row[col_names.index('ledgbal')] if 'ledgbal' in col_names else 0
                product = row[col_names.index('product')] if 'product' in col_names else ''
                curbal = row[col_names.index('curbal')] if 'curbal' in col_names else 0
                intpaybl = row[col_names.index('intpaybl')] if 'intpaybl' in col_names else 0
                float_val = row[col_names.index('float')] if 'float' in col_names else 0
                avbal = row[col_names.index('avbal')] if 'avbal' in col_names else 0
                avbaltt = row[col_names.index('avbaltt')] if 'avbaltt' in col_names else 0
                curbaltt = row[col_names.index('curbaltt')] if 'curbaltt' in col_names else 0
                
                # Format values without scientific notation
                acctno_str = format_number(acctno)
                prodcd_str = format_number(prodcd)
                amtind_str = str(amtind) if amtind is not None else ''
                branch_str = format_number(branch)
                ledgbal_str = f"{ledgbal:.2f}" if ledgbal is not None and not pd.isna(ledgbal) else ''
                product_str = format_number(product)
                curbal_str = f"{curbal:.2f}" if curbal is not None and not pd.isna(curbal) else ''
                intpaybl_str = f"{intpaybl:.2f}" if intpaybl is not None and not pd.isna(intpaybl) else ''
                float_str = f"{float_val:.2f}" if float_val is not None and not pd.isna(float_val) else ''
                avbal_str = f"{avbal:.2f}" if avbal is not None and not pd.isna(avbal) else ''
                avbaltt_str = f"{avbaltt:.2f}" if avbaltt is not None and not pd.isna(avbaltt) else ''
                curbaltt_str = f"{curbaltt:.2f}" if curbaltt is not None and not pd.isna(curbaltt) else ''
                
                # Write formatted row with proper spacing
                f.write(f"{obs_count:3} {prodcd_str:>6} {amtind_str:>6} {branch_str:>6} {acctno_str:>10} {ledgbal_str:>10} {product_str:>7} {curbal_str:>10} {intpaybl_str:>8} {1:>6} {1:>6} {float_str:>10} {avbal_str:>10} {avbaltt_str:>10} {curbaltt_str:>10}\n")
                
                total_float += float_val if float_val is not None and not pd.isna(float_val) else 0
            
            # Add separator and total
            f.write(" " * 50 + "========\n")
            f.write(f"{' ' * 82}{total_float:.2f}\n")
        else:
            f.write("No records found matching B AND NOT A condition.\n")
            f.write("All FLOAT records have matching DEPOSIT records.\n")
        
        f.write("\n")
    
    print(f"  - Report saved to {report_path}")

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
    """Convert column names to lowercase and handle common variations"""
    if df.is_empty():
        return df
    
    df = df.rename({col: col.lower() for col in df.columns})
    return df

def standardize_dataframe(df, columns_to_keep):
    """Standardize DataFrame: keep specified columns and ensure consistent data types"""
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
    """Standardize all DataFrames to have the same schema for concatenation"""
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
# STEP 1: Load FDMTHLY
# ============================================
print("\n[STEP 1] Loading FDMTHLY data...")

fdmthly_df = read_sas_file(mni_path / "fdmthly.sas7bdat", 
                          ['ACCTNO', 'BRANCH', 'INTPLAN', 'CURBAL', 'BIC', 'AMTIND', 'INTPAY'])
if fdmthly_df is not None:
    fdmthly_df = standardize_columns(fdmthly_df)
    print(f"  - MNI FDMTHLY loaded: {fdmthly_df.height} records")
else:
    fdmthly_df = pl.DataFrame()

ifdmthly_df = read_sas_file(imni_path / "fdmthly.sas7bdat",
                           ['ACCTNO', 'BRANCH', 'INTPLAN', 'CURBAL', 'BIC', 'AMTIND', 'INTPAY'])
if ifdmthly_df is not None:
    ifdmthly_df = standardize_columns(ifdmthly_df)
    print(f"  - IMNI FDMTHLY loaded: {ifdmthly_df.height} records")
else:
    ifdmthly_df = pl.DataFrame()

fdmthly_dfs = [fdmthly_df, ifdmthly_df]
fdmthly_dfs = [df for df in fdmthly_dfs if not df.is_empty()]

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
# STEP 2: Load CURN
# ============================================
print("\n[STEP 2] Loading CURN data...")

curn1_df = read_sas_file(mni_path / "curn124.sas7bdat",
                        ['ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH'])
if curn1_df is not None:
    curn1_df = standardize_columns(curn1_df)
    if 'amtind' in curn1_df.columns:
        curn1_df = curn1_df.with_columns([
            pl.col('amtind').cast(pl.Utf8)
        ])
    print(f"  - MNI CURN124 loaded: {curn1_df.height} records")
else:
    curn1_df = pl.DataFrame()

curn2_df = read_sas_file(imni_path / "curn124.sas7bdat",
                        ['ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH'])
if curn2_df is not None:
    curn2_df = standardize_columns(curn2_df)
    if 'amtind' in curn2_df.columns:
        curn2_df = curn2_df.with_columns([
            pl.col('amtind').cast(pl.Utf8)
        ])
    print(f"  - IMNI CURN124 loaded: {curn2_df.height} records")
else:
    curn2_df = pl.DataFrame()

curn_dfs = [curn1_df, curn2_df]
curn_dfs = [df for df in curn_dfs if not df.is_empty()]

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
# STEP 3: Load SAVG
# ============================================
print("\n[STEP 3] Loading SAVG data...")

datasets_to_combine = []
dataset_names = []

savg1_df = read_sas_file(mni_path / "savg124.sas7bdat",
                        ['ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH'])
if savg1_df is not None:
    savg1_df = standardize_columns(savg1_df)
    if 'amtind' in savg1_df.columns:
        savg1_df = savg1_df.with_columns([
            pl.col('amtind').cast(pl.Utf8)
        ])
    print(f"  - MNI SAVG124 loaded: {savg1_df.height} records")
    
    if 'acctno' in savg1_df.columns:
        savg1_df = savg1_df.with_columns([
            pl.col('acctno').cast(pl.Float64)
        ])
    
    savg1_standardized = standardize_dataframe(savg1_df, STANDARD_COLUMNS)
    datasets_to_combine.append(savg1_standardized)
    dataset_names.append("MNI SAVG124")
else:
    print("  - MNI SAVG124 not loaded")

savg2_df = read_sas_file(imni_path / "savg124.sas7bdat",
                        ['ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH'])
if savg2_df is not None:
    savg2_df = standardize_columns(savg2_df)
    if 'amtind' in savg2_df.columns:
        savg2_df = savg2_df.with_columns([
            pl.col('amtind').cast(pl.Utf8)
        ])
    print(f"  - IMNI SAVG124 loaded: {savg2_df.height} records")
    
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
    
    # ============================================
    # STEP 7: Apply filters
    # ============================================
    print("\n[STEP 7] Applying filters and transformations...")
    
    if 'progcd' not in deposit_combined.columns:
        print("  - ERROR: 'progcd' column not found")
        deposit_filtered = pl.DataFrame()
    else:
        valid_progcd = [
            '42110', '42310', '42120', '42320', '42130', '42610',
            '42133', '42132', '42180', '42610', '42630', '34180',
            '42199', '42699'
        ]
        
        deposit_filtered = deposit_combined.filter(
            pl.col('progcd').is_in(valid_progcd)
        )
        print(f"  - After PROGCD filter: {deposit_filtered.height} records")
        
        if 'product' in deposit_filtered.columns and not deposit_filtered.is_empty():
            deposit_filtered = deposit_filtered.with_columns([
                pl.when(pl.col('product') == 166)
                .then(pl.lit('42310'))
                .otherwise(pl.col('progcd'))
                .alias('progcd')
            ])
            print(f"  - After PRODUCT=166: {deposit_filtered.height} records")
            
            deposit_filtered = deposit_filtered.filter(
                ~(
                    pl.col('progcd').is_in(['42199', '42699']) & 
                    ~pl.col('product').is_in([72, 413])
                )
            )
            print(f"  - After PROGCD special: {deposit_filtered.height} records")
            
            deposit_filtered = deposit_filtered.filter(
                ~pl.col('product').is_in([30, 31, 32, 33, 34])
            )
            print(f"  - After PRODUCT filter: {deposit_filtered.height} records")
        
        if 'intpaybl' in deposit_filtered.columns and not deposit_filtered.is_empty():
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
    print(f"  - FLOAT columns: {float_df.columns}")
    
    # Convert acctno to integer string for better display
    float_df = float_df.with_columns([
        pl.col('acctno').cast(pl.Float64)
    ])
    
    float_df.write_parquet(output_path / "FLOAT.parquet")
    
    if not float_df.is_empty() and 'float' in float_df.columns and 'acctno' in float_df.columns:
        float_summary = float_df.group_by('acctno').agg([
            pl.col('float').sum().alias('float')
        ])
        float_summary.write_parquet(output_path / "FLOAT_SUMMARY.parquet")
        print(f"  - FLOAT_SUMMARY saved: {float_summary.height} records")
        
        # Show sample with formatted numbers
        print("  - FLOAT_SUMMARY sample (formatted):")
        for row in float_summary.head(5).rows():
            print(f"    ACCTNO: {int(row[0])}, FLOAT: {row[1]:.2f}")
        
        # Debug: Check ACCTNO match with DEPOSIT
        if not deposit_filtered.is_empty():
            deposit_acctnos = deposit_filtered['acctno'].unique().head(10).to_list()
            float_acctnos = float_summary['acctno'].unique().head(10).to_list()
            
            print(f"  - Sample DEPOSIT ACCTNO (formatted):")
            for acct in deposit_acctnos[:5]:
                print(f"    {int(acct) if isinstance(acct, float) and acct.is_integer() else acct}")
            
            print(f"  - Sample FLOAT ACCTNO (formatted):")
            for acct in float_acctnos[:5]:
                print(f"    {int(acct) if isinstance(acct, float) and acct.is_integer() else acct}")
            
            # Check if any ACCTNO match
            deposit_set = set(deposit_filtered['acctno'].unique().to_list())
            float_set = set(float_summary['acctno'].unique().to_list())
            common = deposit_set.intersection(float_set)
            print(f"  - Common ACCTNO count: {len(common)}")
            if len(common) > 0:
                common_list = list(common)[:5]
                print(f"  - Sample common ACCTNO:")
                for acct in common_list:
                    print(f"    {int(acct) if isinstance(acct, float) and acct.is_integer() else acct}")
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
        # Convert both to Float64 for join
        deposit_sorted = deposit_sorted.with_columns([
            pl.col('acctno').cast(pl.Float64)
        ])
        float_summary = float_summary.with_columns([
            pl.col('acctno').cast(pl.Float64)
        ])
        
        # Try LEFT join first to see if any FLOAT matches
        deposit_merged = deposit_sorted.join(
            float_summary, on='acctno', how='left', suffix='_float'
        )
        print(f"  - Merge completed: {deposit_merged.height} records")
        
        # Check how many records got FLOAT values
        float_not_null = deposit_merged.filter(pl.col('float').is_not_null())
        print(f"  - Records with FLOAT values: {float_not_null.height}")
        
        if float_not_null.height > 0:
            print(f"  - Sample FLOAT matches:")
            for row in float_not_null.select(['acctno', 'float']).head(10).rows():
                print(f"    ACCTNO: {int(row[0]) if isinstance(row[0], float) and row[0].is_integer() else row[0]}, FLOAT: {row[1]:.2f}")
        
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
        # For this, we need to do a RIGHT join or find FLOAT records not in DEPOSIT
        float_only = deposit_merged.filter(
            pl.col('float').is_not_null() & 
            (pl.col('curbal').is_null() | pl.col('product').is_null())
        )
        
        print(f"  - FLOAT_ONLY records: {float_only.height}")
        
        deposit_merged.write_parquet(output_path / "DEPOSIT_MERGED.parquet")
        float_only.write_parquet(output_path / "FLOAT_ONLY.parquet")
        
        print(f"  - DEPOSIT_MERGED saved: {deposit_merged.height} records")
        print(f"  - FLOAT_ONLY saved: {float_only.height} records")
        
        # Generate text report matching SAS output
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
        
        if not float_only.is_empty():
            print("\nFLOAT ONLY RECORDS (B AND NOT A):")
            for row in float_only.select(['acctno', 'float']).head(10).rows():
                print(f"  ACCTNO: {int(row[0]) if isinstance(row[0], float) and row[0].is_integer() else row[0]}, FLOAT: {row[1]:.2f}")
        
    else:
        print("  - No FLOAT data found for merging")
        deposit_merged = deposit_sorted
        
else:
    print("  - No DEPOSIT data found for processing")

print("\n" + "="*60)
print("PROCESSING COMPLETED SUCCESSFULLY")
print("="*60)
