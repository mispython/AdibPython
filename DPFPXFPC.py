import polars as pl
import duckdb
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
        return ''
    if isinstance(val, float):
        if val.is_integer():
            return str(int(val))
        else:
            return f"{val:.2f}"
    return str(val)

def generate_text_report(float_only, output_path):
    """Generate text report matching production SAS output format"""
    print("\n[STEP 10] Generating text report...")
    
    report_path = output_path / "EIFLTEXP_REPORT.txt"
    
    with open(report_path, 'w') as f:
        f.write("The Python Processing System\n")
        f.write(f"{datetime.datetime.now().strftime('%H:%M %A, %B %d, %Y')}\n")
        f.write(" " * 50 + "1\n")
        f.write("\n")
        
        f.write("Obs PRODCD AMTIND BRANCH   ACCTNO   LEDGBAL PRODUCT CURBAL INTPAYBL _TYPE_ _FREQ_   FLOAT    AVBAL    AVBALTT  CURBALTT\n")
        f.write("\n")
        
        if not float_only.is_empty():
            float_only_sorted = float_only.sort('float', ascending=False)
            
            obs_count = 0
            total_float = 0
            
            for row in float_only_sorted.rows():
                obs_count += 1
                
                # Extract values with defaults
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
                
                # Format values
                acctno_str = format_number(acctno)
                prodcd_str = format_number(prodcd) if prodcd else ''
                amtind_str = str(amtind) if amtind else ''
                branch_str = format_number(branch) if branch else ''
                ledgbal_str = f"{ledgbal:.2f}" if ledgbal else ''
                product_str = format_number(product) if product else ''
                curbal_str = f"{curbal:.2f}" if curbal else ''
                intpaybl_str = f"{intpaybl:.2f}" if intpaybl else ''
                float_str = f"{float_val:.2f}" if float_val else ''
                avbal_str = f"{avbal:.2f}" if avbal else ''
                avbaltt_str = f"{avbaltt:.2f}" if avbaltt else ''
                curbaltt_str = f"{curbaltt:.2f}" if curbaltt else ''
                
                # Write formatted row (matching SAS output format)
                f.write(f"{obs_count:3} {prodcd_str:>6} {amtind_str:>6} {branch_str:>6} {acctno_str:>10} {ledgbal_str:>10} {product_str:>7} {curbal_str:>10} {intpaybl_str:>8} {1:>6} {1:>6} {float_str:>10} {avbal_str:>10} {avbaltt_str:>10} {curbaltt_str:>10}\n")
                
                total_float += float_val if float_val else 0
            
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
            # Deduplicate DEPOSIT by ACCTNO (keep first occurrence)
            deposit_filtered = deposit_filtered.unique(subset=['acctno'], keep='first')
            print(f"  - After deduplication by ACCTNO: {deposit_filtered.height} records")
            
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
    
    if 'float' in float_df.columns:
        float_df = float_df.with_columns([
            pl.col('float').cast(pl.Float64)
        ])
    
    if 'acctno' in float_df.columns:
        float_df = float_df.with_columns([
            pl.col('acctno').cast(pl.Float64)
        ])
    
    float_df.write_parquet(output_path / "FLOAT.parquet")
    
    if not float_df.is_empty() and 'float' in float_df.columns and 'acctno' in float_df.columns:
        # Aggregate FLOAT by ACCTNO (sum)
        float_summary = float_df.group_by('acctno').agg([
            pl.col('float').sum().alias('float')
        ])
        float_summary.write_parquet(output_path / "FLOAT_SUMMARY.parquet")
        print(f"  - FLOAT_SUMMARY saved: {float_summary.height} records")
        
        print("\n  - Sample FLOAT data:")
        for row in float_summary.head(5).rows():
            print(f"    ACCTNO: {int(row[0])}, FLOAT: {row[1]:.2f}")
    else:
        float_summary = pl.DataFrame()
else:
    float_df = pl.DataFrame()
    float_summary = pl.DataFrame()

# ============================================
# STEP 9: Merge DEPOSIT with FLOAT (B AND NOT A)
# ============================================
print("\n[STEP 9] Merging DEPOSIT with FLOAT...")

if not deposit_filtered.is_empty() and not float_summary.is_empty():
    deposit_sorted = deposit_filtered.sort('acctno')
    float_summary = float_summary.sort('acctno')
    
    print(f"\n  - DEPOSIT unique ACCTNO count: {deposit_sorted.height}")
    print(f"  - FLOAT unique ACCTNO count: {float_summary.height}")
    
    # Extract last 6 digits for matching
    deposit_with_key = deposit_sorted.with_columns([
        pl.col('acctno').cast(pl.Utf8).str.slice(-6).alias('acctno_key')
    ])
    
    float_with_key = float_summary.with_columns([
        pl.col('acctno').cast(pl.Utf8).str.slice(-6).alias('acctno_key')
    ])
    
    # Get FLOAT keys that are NOT in DEPOSIT (B AND NOT A)
    deposit_keys = set(deposit_with_key['acctno_key'].unique().to_list())
    float_keys = set(float_with_key['acctno_key'].unique().to_list())
    float_only_keys = float_keys - deposit_keys
    
    print(f"\n  - FLOAT keys not in DEPOSIT: {len(float_only_keys)}")
    
    # Create FLOAT_ONLY records (matching SAS IF B AND NOT A)
    if float_only_keys:
        float_only = float_with_key.filter(
            pl.col('acctno_key').is_in(list(float_only_keys))
        ).select([
            pl.col('acctno').alias('acctno'),
            pl.col('float').alias('float'),
            pl.lit(None).cast(pl.Utf8).alias('progcd'),
            pl.lit(None).cast(pl.Utf8).alias('amtind'),
            pl.lit(None).cast(pl.Float64).alias('branch'),
            pl.lit(0).cast(pl.Float64).alias('ledgbal'),
            pl.lit(0).cast(pl.Float64).alias('product'),
            pl.lit(0).cast(pl.Float64).alias('curbal'),
            pl.lit(0).cast(pl.Float64).alias('intpaybl'),
            (pl.col('float') * -1).alias('avbal'),  # AVBAL = -FLOAT (since CURBAL is 0)
            (pl.col('float') * -1).alias('avbaltt'),  # AVBALTT = -FLOAT (since INTPAYBL is 0)
            pl.lit(0).cast(pl.Float64).alias('curbaltt')  # CURBALTT = 0
        ])
        
        print(f"  - FLOAT_ONLY records: {float_only.height}")
        
        # Show sample of FLOAT_ONLY records
        print("\n  - Sample FLOAT_ONLY records:")
        for row in float_only.head(10).rows():
            print(f"    ACCTNO: {int(row[0])}, FLOAT: {row[1]:.2f}")
        
        # Generate text report with only FLOAT_ONLY records
        generate_text_report(float_only, output_path)
        
        # Save FLOAT_ONLY records
        float_only.write_parquet(output_path / "FLOAT_ONLY.parquet")
        print(f"  - FLOAT_ONLY saved: {float_only.height} records")
        
        # Also create the merged dataset for completeness (but the report only shows FLOAT_ONLY)
        float_aggregated = float_with_key.group_by('acctno_key').agg([
            pl.col('float').sum().alias('float'),
            pl.col('acctno').first().alias('acctno_original')
        ])
        
        deposit_merged = deposit_with_key.join(
            float_aggregated,
            on='acctno_key',
            how='left',
            suffix='_float'
        )
        
        # IF CURBAL < 0 THEN CURBAL = 0;
        if 'curbal' in deposit_merged.columns:
            deposit_merged = deposit_merged.with_columns([
                pl.when(pl.col('curbal') < 0)
                .then(0)
                .otherwise(pl.col('curbal'))
                .alias('curbal')
            ])
        
        # AVBAL = SUM(CURBAL,(-1)*FLOAT);
        if 'curbal' in deposit_merged.columns and 'float' in deposit_merged.columns:
            deposit_merged = deposit_merged.with_columns([
                (pl.col('curbal') + (-1) * pl.col('float')).alias('avbal')
            ])
        
        # AVBALTT = SUM(AVBAL,INTPAYBL);
        # CURBALTT = SUM(CURBAL,INTPAYBL);
        if 'avbal' in deposit_merged.columns and 'curbal' in deposit_merged.columns and 'intpaybl' in deposit_merged.columns:
            deposit_merged = deposit_merged.with_columns([
                (pl.col('avbal') + pl.col('intpaybl')).alias('avbaltt'),
                (pl.col('curbal') + pl.col('intpaybl')).alias('curbaltt')
            ])
        
        # Clean up
        if 'acctno_key' in deposit_merged.columns:
            deposit_merged = deposit_merged.drop('acctno_key')
        
        deposit_merged.write_parquet(output_path / "DEPOSIT_MERGED.parquet")
        print(f"  - DEPOSIT_MERGED saved: {deposit_merged.height} records")
        
        print("\n" + "="*60)
        print("DEPOSIT DATA WITH FLOAT SUMMARY")
        print("="*60)
        if not deposit_merged.is_empty():
            total_float = deposit_merged.select(pl.col('float').sum()).row(0)[0]
            total_avbal = deposit_merged.select(pl.col('avbal').sum()).row(0)[0]
            print(f"Total FLOAT: {total_float:,.2f}")
            print(f"Total AVBAL: {total_avbal:,.2f}")
            print(f"Total Records: {deposit_merged.height}")
        
    else:
        print("  - No FLOAT_ONLY records found (all FLOAT records have matching DEPOSIT)")
        float_only = pl.DataFrame()
        generate_text_report(float_only, output_path)
        
else:
    print("  - No DEPOSIT or FLOAT data to merge")

print("\n" + "="*60)
print("PROCESSING COMPLETED SUCCESSFULLY")
print("="*60)
