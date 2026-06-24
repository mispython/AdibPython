import polars as pl
import duckdb
from pathlib import Path

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

# ============================================
# STEP 1: Load FDMTHLY from MNI and IMNI
# ============================================
print("\n[STEP 1] Loading FDMTHLY data...")

try:
    fdmthly_df = pl.read_parquet(mni_path / "FDMTHLY.parquet").select([
        'ACCTNO', 'BRANCH', 'INTPLAN', 'CURBAL', 'BIC', 'AMTIND', 'INTPAY'
    ]).sort('ACCTNO')
    print(f"  - MNI FDMTHLY loaded: {fdmthly_df.height} records")
except FileNotFoundError:
    print(f"  - WARNING: MNI/FDMTHLY.parquet not found")
    fdmthly_df = pl.DataFrame()
except Exception as e:
    print(f"  - ERROR reading MNI/FDMTHLY.parquet: {e}")
    fdmthly_df = pl.DataFrame()

try:
    ifdmthly_df = pl.read_parquet(imni_path / "FDMTHLY.parquet").select([
        'ACCTNO', 'BRANCH', 'INTPLAN', 'CURBAL', 'BIC', 'AMTIND', 'INTPAY'
    ]).sort('ACCTNO')
    print(f"  - IMNI FDMTHLY loaded: {ifdmthly_df.height} records")
except FileNotFoundError:
    print(f"  - WARNING: IMNI/FDMTHLY.parquet not found")
    ifdmthly_df = pl.DataFrame()
except Exception as e:
    print(f"  - ERROR reading IMNI/FDMTHLY.parquet: {e}")
    ifdmthly_df = pl.DataFrame()

# DATA FDMTHLY; SET FDMTHLY IFDMTHLY;
fdmthly_combined = pl.concat([fdmthly_df, ifdmthly_df], how="diagonal")
print(f"  - Combined FDMTHLY: {fdmthly_combined.height} records")

if not fdmthly_combined.is_empty():
    fdmthly_processed = fdmthly_combined.with_columns([
        pl.col('CURBAL').alias('LEDGBAL')
    ])
    fdmthly_processed.write_parquet(output_path / "FDMTHLY.parquet")
    print(f"  - FDMTHLY saved to {output_path / 'FDMTHLY.parquet'}")
else:
    fdmthly_processed = pl.DataFrame()
    print("  - No FDMTHLY data to save")

# ============================================
# STEP 2: Load CURN from MNI and IMNI
# ============================================
print("\n[STEP 2] Loading CURN data...")

try:
    curn1_df = pl.read_parquet(mni_path / "CURN124.parquet")
    print(f"  - MNI CURN124 loaded: {curn1_df.height} records")
except FileNotFoundError:
    print(f"  - WARNING: MNI/CURN124.parquet not found")
    curn1_df = pl.DataFrame()
except Exception as e:
    print(f"  - ERROR reading MNI/CURN124.parquet: {e}")
    curn1_df = pl.DataFrame()

try:
    curn2_df = pl.read_parquet(imni_path / "CURN124.parquet")
    print(f"  - IMNI CURN124 loaded: {curn2_df.height} records")
except FileNotFoundError:
    print(f"  - WARNING: IMNI/CURN124.parquet not found")
    curn2_df = pl.DataFrame()
except Exception as e:
    print(f"  - ERROR reading IMNI/CURN124.parquet: {e}")
    curn2_df = pl.DataFrame()

curn_combined = pl.concat([curn1_df, curn2_df], how="diagonal")
print(f"  - Combined CURN: {curn_combined.height} records")

# IF PRODUCT = 139 THEN DELETE;
if not curn_combined.is_empty():
    # Check if 'PRODUCT' column exists
    if 'PRODUCT' in curn_combined.columns:
        curn_filtered = curn_combined.filter(pl.col('PRODUCT') != 139)
        curn_filtered.write_parquet(output_path / "CURN.parquet")
        print(f"  - CURN filtered (removed PRODUCT=139): {curn_filtered.height} records")
    else:
        print("  - WARNING: 'PRODUCT' column not found in CURN data")
        curn_filtered = curn_combined
        curn_filtered.write_parquet(output_path / "CURN.parquet")
else:
    curn_filtered = pl.DataFrame()
    print("  - No CURN data to save")

# ============================================
# STEP 3: Load SAVG data and combine
# ============================================
print("\n[STEP 3] Loading SAVG data...")

datasets_to_combine = []
dataset_names = []

# MNI.SAVG124
try:
    savg1_df = pl.read_parquet(mni_path / "SAVG124.parquet")
    print(f"  - MNI SAVG124 loaded: {savg1_df.height} records")
    # Check available columns
    print(f"    Columns: {savg1_df.columns}")
    
    # Select available columns
    available_cols = ['ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH']
    existing_cols = [col for col in available_cols if col in savg1_df.columns]
    savg1_selected = savg1_df.select(existing_cols)
    datasets_to_combine.append(savg1_selected)
    dataset_names.append("MNI SAVG124")
except FileNotFoundError:
    print(f"  - WARNING: MNI/SAVG124.parquet not found")
except Exception as e:
    print(f"  - ERROR reading MNI/SAVG124.parquet: {e}")

# IMNI.SAVG124
try:
    savg2_df = pl.read_parquet(imni_path / "SAVG124.parquet")
    print(f"  - IMNI SAVG124 loaded: {savg2_df.height} records")
    available_cols = ['ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH']
    existing_cols = [col for col in available_cols if col in savg2_df.columns]
    savg2_selected = savg2_df.select(existing_cols)
    datasets_to_combine.append(savg2_selected)
    dataset_names.append("IMNI SAVG124")
except FileNotFoundError:
    print(f"  - WARNING: IMNI/SAVG124.parquet not found")
except Exception as e:
    print(f"  - ERROR reading IMNI/SAVG124.parquet: {e}")

# ============================================
# STEP 4: Add CURN to datasets
# ============================================
print("\n[STEP 4] Adding CURN to dataset list...")

if not curn_filtered.is_empty():
    available_cols = ['ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH']
    existing_cols = [col for col in available_cols if col in curn_filtered.columns]
    curn_selected = curn_filtered.select(existing_cols)
    datasets_to_combine.append(curn_selected)
    dataset_names.append("CURN")
    print(f"  - CURN added with {curn_selected.height} records")
else:
    print("  - No CURN data to add")

# ============================================
# STEP 5: Add FDMTHLY to datasets
# ============================================
print("\n[STEP 5] Adding FDMTHLY to dataset list...")

if not fdmthly_processed.is_empty():
    # FDMTHLY has different column names - map them
    fdmthly_renamed = fdmthly_processed.select([
        'ACCTNO', 'BRANCH', 'CURBAL', 'LEDGBAL', 'AMTIND'
    ]).with_columns([
        pl.col('INTPLAN').alias('PRODUCT'),
        pl.col('BIC').alias('PRODCD'),
        pl.col('INTPAY').alias('INTPAYBL')
    ])
    datasets_to_combine.append(fdmthly_renamed)
    dataset_names.append("FDMTHLY")
    print(f"  - FDMTHLY added with {fdmthly_renamed.height} records")
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
    deposit_combined = pl.concat(datasets_to_combine, how="diagonal")
    print(f"  - Combined data: {deposit_combined.height} records")
    print(f"  - Columns: {deposit_combined.columns}")
    
    # ============================================
    # STEP 7: Apply filters and transformations
    # ============================================
    print("\n[STEP 7] Applying filters and transformations...")
    
    # Check if required columns exist
    if 'PRODCD' not in deposit_combined.columns:
        print("  - ERROR: 'PRODCD' column not found in combined data")
        print(f"  - Available columns: {deposit_combined.columns}")
        deposit_filtered = pl.DataFrame()
    else:
        valid_prodcd = [
            '42110', '42310', '42120', '42320', '42130', '42610',
            '42133', '42132', '42180', '42610', '42630', '34180',
            '42199', '42699'
        ]
        
        print(f"  - Valid PRODCD values: {valid_prodcd}")
        
        # Apply filters
        deposit_filtered = deposit_combined.filter(
            pl.col('PRODCD').is_in(valid_prodcd)
        )
        print(f"  - After PRODCD filter: {deposit_filtered.height} records")
        
        # Check if 'PRODUCT' column exists for additional filters
        if 'PRODUCT' in deposit_filtered.columns:
            deposit_filtered = deposit_filtered.with_columns([
                pl.when(pl.col('PRODUCT') == 166)
                .then(pl.lit('42310'))
                .otherwise(pl.col('PRODCD'))
                .alias('PRODCD')
            ])
            print(f"  - After PRODUCT=166 transformation: {deposit_filtered.height} records")
            
            deposit_filtered = deposit_filtered.filter(
                ~(
                    pl.col('PRODCD').is_in(['42199', '42699']) & 
                    ~pl.col('PRODUCT').is_in([72, 413])
                )
            )
            print(f"  - After PRODCD special filter: {deposit_filtered.height} records")
            
            deposit_filtered = deposit_filtered.filter(
                ~pl.col('PRODUCT').is_in([30, 31, 32, 33, 34])
            )
            print(f"  - After PRODUCT filter: {deposit_filtered.height} records")
        else:
            print("  - WARNING: 'PRODUCT' column not found, skipping PRODUCT-based filters")
        
        # Handle INTPAYBL
        if 'INTPAYBL' in deposit_filtered.columns:
            deposit_filtered = deposit_filtered.with_columns([
                pl.when(pl.col('INTPAYBL') < 0)
                .then(0)
                .otherwise(pl.col('INTPAYBL'))
                .alias('INTPAYBL')
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

try:
    float_df = pl.read_parquet(pidms_path / "FLOAT.parquet")
    print(f"  - FLOAT loaded: {float_df.height} records")
    float_df.write_parquet(output_path / "FLOAT.parquet")
    
    # PROC SUMMARY DATA=FLOAT NWAY; CLASS ACCTNO; VAR FLOAT; OUTPUT OUT=FLOAT SUM=;
    if not float_df.is_empty() and 'FLOAT' in float_df.columns:
        float_summary = float_df.group_by('ACCTNO').agg([
            pl.col('FLOAT').sum().alias('FLOAT')
        ])
        float_summary.write_parquet(output_path / "FLOAT_SUMMARY.parquet")
        print(f"  - FLOAT_SUMMARY saved: {float_summary.height} records")
    else:
        float_summary = pl.DataFrame()
        print("  - FLOAT column not found in FLOAT data")
        
except FileNotFoundError:
    print(f"  - WARNING: {pidms_path / 'FLOAT.parquet'} not found")
    float_df = pl.DataFrame()
    float_summary = pl.DataFrame()
except Exception as e:
    print(f"  - ERROR reading FLOAT.parquet: {e}")
    float_df = pl.DataFrame()
    float_summary = pl.DataFrame()

# ============================================
# STEP 9: Merge DEPOSIT with FLOAT
# ============================================
print("\n[STEP 9] Merging DEPOSIT with FLOAT...")

if not deposit_filtered.is_empty():
    deposit_sorted = deposit_filtered.sort('ACCTNO')
    
    if not float_summary.is_empty():
        deposit_merged = deposit_sorted.join(
            float_summary, on='ACCTNO', how='left', suffix='_float'
        )
        print(f"  - Merge completed: {deposit_merged.height} records")
        
        # Apply calculations
        deposit_merged = deposit_merged.with_columns([
            pl.when(pl.col('CURBAL') < 0)
            .then(0)
            .otherwise(pl.col('CURBAL'))
            .alias('CURBAL'),
            
            (pl.col('CURBAL') + (-1) * pl.col('FLOAT')).alias('AVBAL'),
            (pl.col('AVBAL') + pl.col('INTPAYBL')).alias('AVBALTT'),
            (pl.col('CURBAL') + pl.col('INTPAYBL')).alias('CURBALTT')
        ])
        
        # IF B AND NOT A; (keep records that are in FLOAT but not in DEPOSIT)
        float_only = deposit_merged.filter(
            pl.col('FLOAT').is_not_null() & 
            (pl.col('CURBAL').is_null() | pl.col('PRODUCT').is_null())
        )
        
        deposit_merged.write_parquet(output_path / "DEPOSIT_MERGED.parquet")
        float_only.write_parquet(output_path / "FLOAT_ONLY.parquet")
        
        print(f"  - DEPOSIT_MERGED saved: {deposit_merged.height} records")
        print(f"  - FLOAT_ONLY saved: {float_only.height} records")
        
        # PROC PRINT DATA=DEPOSIT; SUM FLOAT;
        print("\n" + "="*60)
        print("DEPOSIT DATA WITH FLOAT SUMMARY")
        print("="*60)
        print(f"Total FLOAT: {deposit_merged.select(pl.col('FLOAT').sum()).row(0)[0]:,.2f}")
        print(f"Total AVBAL: {deposit_merged.select(pl.col('AVBAL').sum()).row(0)[0]:,.2f}")
        
        print("\nFLOAT ONLY RECORDS (B AND NOT A):")
        print(f"  - Records: {float_only.height}")
        if not float_only.is_empty():
            print(float_only.select(['ACCTNO', 'FLOAT']).head(10))
        
    else:
        print("  - No FLOAT data found for merging")
        deposit_merged = deposit_sorted
        
else:
    print("  - No DEPOSIT data found for processing")

print("\n" + "="*60)
print("PROCESSING COMPLETED SUCCESSFULLY")
print("="*60)
