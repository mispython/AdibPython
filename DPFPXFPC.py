import polars as pl
from pathlib import Path
import pyreadstat
import datetime
import pandas as pd

# ============================================
# CONFIGURATION
# ============================================

# Set to 'BOTH' to match production (process both Conventional and Islamic)
PROCESS_TYPE = 'BOTH'  # Options: 'BOTH', 'CONVENTIONAL', 'ISLAMIC'

mni_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/MNI")
imni_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/IMNI")
pidms_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/PIDM")
output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFLTEXP")
output_path.mkdir(exist_ok=True)

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
    """Generate text report matching production SAS output format exactly"""
    print("\n[STEP 10] Generating text report...")
    
    report_path = output_path / "EIFLTEXP_REPORT.txt"
    
    with open(report_path, 'w') as f:
        f.write("The Python Processing System\n")
        f.write(f"{datetime.datetime.now().strftime('%H:%M %A, %B %d, %Y')}\n")
        f.write(" " * 50 + "1\n")
        f.write("\n")
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
                
                acctno_str = str(int(acctno)) if acctno and not pd.isna(acctno) else ''
                prodcd_str = '' if prodcd is None or (isinstance(prodcd, float) and pd.isna(prodcd)) else str(prodcd)
                amtind_str = '' if amtind is None or (isinstance(amtind, float) and pd.isna(amtind)) else str(amtind)
                branch_str = '' if branch is None or (isinstance(branch, float) and pd.isna(branch)) else str(int(branch))
                ledgbal_str = '' if ledgbal is None or (isinstance(ledgbal, float) and pd.isna(ledgbal)) else f"{ledgbal:.2f}"
                product_str = '' if product is None or (isinstance(product, float) and pd.isna(product)) else str(int(product))
                curbal_str = '0' if curbal is None or (isinstance(curbal, float) and pd.isna(curbal)) else str(int(curbal)) if curbal.is_integer() else f"{curbal:.2f}"
                intpaybl_str = '' if intpaybl is None or (isinstance(intpaybl, float) and pd.isna(intpaybl)) else f"{intpaybl:.2f}"
                float_str = f"{float_val:.2f}" if float_val and not pd.isna(float_val) else ''
                avbal_str = f"{avbal:.2f}" if avbal and not pd.isna(avbal) else ''
                avbaltt_str = f"{avbaltt:.2f}" if avbaltt and not pd.isna(avbaltt) else ''
                curbaltt_str = '0' if curbaltt is None or (isinstance(curbaltt, float) and pd.isna(curbaltt)) else str(int(curbaltt)) if curbaltt.is_integer() else f"{curbaltt:.2f}"
                
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
                pl.lit(None).cast(pl.Int64 if col in ['acctno', 'branch'] else (pl.Float64 if col in ['curbal', 'ledgbal', 'intpaybl'] else pl.Utf8)).alias(col)
            ])
    
    if 'acctno' in df.columns:
        df = df.with_columns([
            pl.col('acctno').cast(pl.Int64)
        ])
    
    if 'branch' in df.columns:
        df = df.with_columns([
            pl.col('branch').cast(pl.Int64)
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
                elif col in ['acctno', 'branch']:
                    df = df.with_columns([
                        pl.lit(None).cast(pl.Int64).alias(col)
                    ])
                else:
                    df = df.with_columns([
                        pl.lit(None).cast(pl.Float64 if col in ['curbal', 'ledgbal', 'intpaybl'] else pl.Utf8).alias(col)
                    ])
        
        if 'acctno' in df.columns:
            try:
                df = df.with_columns([
                    pl.col('acctno').cast(pl.Int64)
                ])
            except:
                pass
        
        if 'branch' in df.columns:
            try:
                df = df.with_columns([
                    pl.col('branch').cast(pl.Int64)
                ])
            except:
                pass
        
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
        
        df = df.select(all_cols)
        standardized_dfs.append(df)
    
    return standardized_dfs

# ============================================
# MAIN PROCESSING
# ============================================

print("="*60)
print(f"EIFLTEXP PROCESSING STARTED - {PROCESS_TYPE}")
print("="*60)
print(f"MNI Path: {mni_path}")
print(f"IMNI Path: {imni_path}")
print(f"PIDM Path: {pidms_path}")
print(f"Output Path: {output_path}")
print("="*60)

STANDARD_COLUMNS = ['acctno', 'branch', 'curbal', 'ledgbal', 'amtind', 'intpaybl', 'product', 'progcd']

# ============================================
# STEP 1: Load and combine FDMTHLY
# ============================================
print("\n[STEP 1] Loading FDMTHLY data...")

fdmthly_dfs = []

if PROCESS_TYPE in ['BOTH', 'CONVENTIONAL']:
    df = read_sas_file(mni_path / "fdmthly.sas7bdat", 
                      ['ACCTNO', 'BRANCH', 'INTPLAN', 'CURBAL', 'BIC', 'AMTIND', 'INTPAY'])
    if df is not None:
        df = standardize_columns(df)
        print(f"  - MNI FDMTHLY loaded: {df.height} records")
        fdmthly_dfs.append(df)

if PROCESS_TYPE in ['BOTH', 'ISLAMIC']:
    df = read_sas_file(imni_path / "fdmthly.sas7bdat",
                      ['ACCTNO', 'BRANCH', 'INTPLAN', 'CURBAL', 'BIC', 'AMTIND', 'INTPAY'])
    if df is not None:
        df = standardize_columns(df)
        print(f"  - IMNI FDMTHLY loaded: {df.height} records")
        fdmthly_dfs.append(df)

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
    fdmthly_processed.write_parquet(output_path / "FDMTHLY.parquet")
    print(f"  - FDMTHLY saved")
else:
    fdmthly_processed = pl.DataFrame()
    print("  - No FDMTHLY data")

# ============================================
# STEP 2: Load and combine CURN
# ============================================
print("\n[STEP 2] Loading CURN data...")

curn_dfs = []

if PROCESS_TYPE in ['BOTH', 'CONVENTIONAL']:
    df = read_sas_file(mni_path / "curn124.sas7bdat",
                      ['ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH'])
    if df is not None:
        df = standardize_columns(df)
        if 'amtind' in df.columns:
            df = df.with_columns([pl.col('amtind').cast(pl.Utf8)])
        print(f"  - MNI CURN124 loaded: {df.height} records")
        curn_dfs.append(df)

if PROCESS_TYPE in ['BOTH', 'ISLAMIC']:
    df = read_sas_file(imni_path / "curn124.sas7bdat",
                      ['ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH'])
    if df is not None:
        df = standardize_columns(df)
        if 'amtind' in df.columns:
            df = df.with_columns([pl.col('amtind').cast(pl.Utf8)])
        print(f"  - IMNI CURN124 loaded: {df.height} records")
        curn_dfs.append(df)

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
            pl.col('acctno').cast(pl.Int64)
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

if PROCESS_TYPE in ['BOTH', 'CONVENTIONAL']:
    df = read_sas_file(mni_path / "savg124.sas7bdat",
                      ['ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH'])
    if df is not None:
        df = standardize_columns(df)
        if 'amtind' in df.columns:
            df = df.with_columns([pl.col('amtind').cast(pl.Utf8)])
        print(f"  - MNI SAVG124 loaded: {df.height} records")
        
        if 'acctno' in df.columns:
            df = df.with_columns([pl.col('acctno').cast(pl.Int64)])
        
        df_standardized = standardize_dataframe(df, STANDARD_COLUMNS)
        datasets_to_combine.append(df_standardized)
        dataset_names.append("MNI SAVG124")

if PROCESS_TYPE in ['BOTH', 'ISLAMIC']:
    df = read_sas_file(imni_path / "savg124.sas7bdat",
                      ['ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH'])
    if df is not None:
        df = standardize_columns(df)
        if 'amtind' in df.columns:
            df = df.with_columns([pl.col('amtind').cast(pl.Utf8)])
        print(f"  - IMNI SAVG124 loaded: {df.height} records")
        
        if 'acctno' in df.columns:
            df = df.with_columns([pl.col('acctno').cast(pl.Int64)])
        
        df_standardized = standardize_dataframe(df, STANDARD_COLUMNS)
        datasets_to_combine.append(df_standardized)
        dataset_names.append("IMNI SAVG124")

# ============================================
# STEP 4: Add CURN
# ============================================
print("\n[STEP 4] Adding CURN to dataset list...")

if not curn_filtered.is_empty():
    curn_standardized = standardize_dataframe(curn_filtered, STANDARD_COLUMNS)
    datasets_to_combine.append(curn_standardized)
    dataset_names.append("CURN")
    print(f"  - CURN added with {curn_standardized.height} records")

# ============================================
# STEP 5: Add FDMTHLY
# ============================================
print("\n[STEP 5] Adding FDMTHLY to dataset list...")

if not fdmthly_processed.is_empty():
    datasets_to_combine.append(fdmthly_processed)
    dataset_names.append("FDMTHLY")
    print(f"  - FDMTHLY added with {fdmthly_processed.height} records")

# ============================================
# STEP 6: Combine all datasets
# ============================================
print("\n[STEP 6] Combining all datasets...")

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
        if deposit_combined['progcd'].dtype != pl.Utf8:
            deposit_combined = deposit_combined.with_columns([
                pl.col('progcd').cast(pl.Utf8)
            ])
        
        if deposit_combined['product'].dtype != pl.Float64:
            deposit_combined = deposit_combined.with_columns([
                pl.col('product').cast(pl.Float64)
            ])
        
        if deposit_combined['intpaybl'].dtype != pl.Float64:
            deposit_combined = deposit_combined.with_columns([
                pl.col('intpaybl').cast(pl.Float64)
            ])
        
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
            
            deposit_filtered = deposit_filtered.filter(
                ~(
                    pl.col('progcd').is_in(['42199', '42699']) & 
                    ~pl.col('product').is_in([72.0, 413.0])
                )
            )
            
            deposit_filtered = deposit_filtered.filter(
                ~pl.col('product').is_in([30.0, 31.0, 32.0, 33.0, 34.0])
            )
        
        if 'intpaybl' in deposit_filtered.columns and not deposit_filtered.is_empty():
            deposit_filtered = deposit_filtered.with_columns([
                pl.when(pl.col('intpaybl') < 0)
                .then(0)
                .otherwise(pl.col('intpaybl'))
                .alias('intpaybl')
            ])
        
        if not deposit_filtered.is_empty():
            deposit_filtered = deposit_filtered.unique(subset=['acctno'], keep='first')
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
    
    if 'float' in float_df.columns:
        float_df = float_df.with_columns([pl.col('float').cast(pl.Float64)])
    
    if 'acctno' in float_df.columns:
        float_df = float_df.with_columns([pl.col('acctno').cast(pl.Int64)])
    
    float_df.write_parquet(output_path / "FLOAT.parquet")
    
    if not float_df.is_empty() and 'float' in float_df.columns and 'acctno' in float_df.columns:
        float_summary = float_df.group_by('acctno').agg([
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
# STEP 9: Find B AND NOT A
# ============================================
print("\n[STEP 9] Finding FLOAT records not in DEPOSIT (B AND NOT A)...")

if not deposit_filtered.is_empty() and not float_summary.is_empty():
    if deposit_filtered['acctno'].dtype != pl.Int64:
        deposit_filtered = deposit_filtered.with_columns([
            pl.col('acctno').cast(pl.Int64)
        ])
    
    if float_summary['acctno'].dtype != pl.Int64:
        float_summary = float_summary.with_columns([
            pl.col('acctno').cast(pl.Int64)
        ])
    
    deposit_acctnos = set(deposit_filtered['acctno'].unique().to_list())
    float_acctnos = set(float_summary['acctno'].unique().to_list())
    
    overlap = deposit_acctnos.intersection(float_acctnos)
    print(f"  - DEPOSIT unique ACCTNO count: {len(deposit_acctnos)}")
    print(f"  - FLOAT unique ACCTNO count: {len(float_acctnos)}")
    print(f"  - Overlap ACCTNO count: {len(overlap)}")
    
    float_only = float_summary.filter(
        ~pl.col('acctno').is_in(list(deposit_acctnos))
    )
    
    print(f"  - FLOAT records not in DEPOSIT (B AND NOT A): {float_only.height}")
    
    if not float_only.is_empty():
        float_only_output = float_only.with_columns([
            pl.lit(None).cast(pl.Utf8).alias('progcd'),
            pl.lit(None).cast(pl.Utf8).alias('amtind'),
            pl.lit(None).cast(pl.Int64).alias('branch'),
            pl.lit(None).cast(pl.Float64).alias('ledgbal'),
            pl.lit(None).cast(pl.Float64).alias('product'),
            pl.lit(0).cast(pl.Float64).alias('curbal'),
            pl.lit(None).cast(pl.Float64).alias('intpaybl'),
            (-pl.col('float')).alias('avbal'),
            (-pl.col('float')).alias('avbaltt'),
            pl.lit(0).cast(pl.Float64).alias('curbaltt')
        ]).select([
            'acctno', 'float', 'progcd', 'amtind', 'branch', 
            'ledgbal', 'product', 'curbal', 'intpaybl',
            'avbal', 'avbaltt', 'curbaltt'
        ])
        
        generate_text_report(float_only_output, output_path)
        
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
