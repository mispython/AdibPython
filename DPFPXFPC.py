import polars as pl
from pathlib import Path
import pyreadstat
import datetime
import pandas as pd

# ============================================
# CONFIGURATION
# ============================================

mni_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/MNI")
imni_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/IMNI")
pidms_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/PIDM")
output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFLTEXP")
output_path.mkdir(exist_ok=True)

VALID_PROGCD = ['42110','42310','42120','42320','42130','42610','42133','42132','42180','42610','42630','34180','42199','42699']
STANDARD_COLS = ['acctno','branch','curbal','ledgbal','amtind','intpaybl','product','progcd']

# ============================================
# HELPER FUNCTIONS
# ============================================

def read_sas(filepath, cols=None):
    """Read SAS file and return Polars DataFrame with lowercase columns"""
    try:
        print(f"    Reading: {filepath.name}")
        df, meta = pyreadstat.read_sas7bdat(str(filepath))
        df = pl.from_pandas(df)
        
        # Convert column names to lowercase
        df = df.rename({c: c.lower() for c in df.columns})
        
        # Select only needed columns
        if cols:
            cols_lower = [c.lower() for c in cols]
            available_cols = [c for c in cols_lower if c in df.columns]
            if available_cols:
                df = df.select(available_cols)
            else:
                print(f"      WARNING: No matching columns found")
                return pl.DataFrame()
        
        print(f"      Loaded: {df.height:,} records")
        return df
    except FileNotFoundError:
        print(f"      WARNING: {filepath.name} not found")
        return pl.DataFrame()
    except Exception as e:
        print(f"      ERROR reading {filepath.name}: {e}")
        return pl.DataFrame()

def standardize(df):
    """Standardize column names and types"""
    if df.is_empty():
        return df
    
    # Add missing columns
    for col in STANDARD_COLS:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))
    
    # Convert types
    if 'acctno' in df.columns:
        try:
            df = df.with_columns(pl.col('acctno').cast(pl.Int64))
        except:
            pass
    
    if 'branch' in df.columns:
        try:
            df = df.with_columns(pl.col('branch').cast(pl.Int64))
        except:
            pass
    
    for col in ['curbal', 'ledgbal', 'intpaybl']:
        if col in df.columns:
            try:
                df = df.with_columns(pl.col(col).cast(pl.Float64))
            except:
                try:
                    df = df.with_columns(pl.col(col).cast(pl.Utf8).str.replace_all(',', '').cast(pl.Float64))
                except:
                    pass
    
    return df.select(STANDARD_COLS)

def generate_report(float_only, output_path):
    """Generate text report matching SAS output"""
    report_path = output_path / "EIFLTEXP_REPORT.txt"
    
    with open(report_path, 'w') as f:
        f.write("The Python Processing System\n")
        f.write(f"{datetime.datetime.now().strftime('%H:%M %A, %B %d, %Y')}\n")
        f.write(" " * 50 + "1\n\n")
        f.write("Obs PRODCD AMTIND BRANCH   ACCTNO   LEDGBAL PRODUCT CURBAL INTPAYBL _TYPE_ _FREQ_   FLOAT    AVBAL    AVBALTT  CURBALTT\n")
        f.write("                                                                                                                       \n")
        
        if not float_only.is_empty():
            obs = 0
            total = 0
            for row in float_only.sort('acctno').rows():
                obs += 1
                acctno, float_val = row[0], row[1]
                total += float_val
                
                acctno_str = str(int(acctno))
                float_str = f"{float_val:.2f}"
                avbal_str = f"{-float_val:.2f}"
                
                f.write(f"{obs:3}          {acctno_str:>10}                        0       {float_str:>10} {avbal_str:>10} {avbal_str:>10}        0\n")
            
            f.write(" " * 50 + "========\n")
            f.write(f"{' ' * 87}{total:.2f}\n")
        else:
            f.write("No records found matching B AND NOT A condition.\n")
            f.write("All FLOAT records have matching DEPOSIT records.\n")
    
    print(f"  - Report: {report_path}")

# ============================================
# STEP 1: Build DEPOSIT Dataset
# ============================================
print("\n" + "="*60)
print("EIFLTEXP PROCESSING STARTED")
print("="*60)
print("\n[STEP 1] Building DEPOSIT dataset...")

# Load all source datasets
print("  Loading MNI SAVG124...")
savg = read_sas(mni_path / "savg124.sas7bdat", ['acctno','product','curbal','ledgbal','prodcd','amtind','intpaybl','branch'])
# Rename prodcd to progcd for SAVG
if not savg.is_empty() and 'prodcd' in savg.columns:
    savg = savg.rename({'prodcd': 'progcd'})

print("  Loading IMNI SAVG124...")
isavg = read_sas(imni_path / "savg124.sas7bdat", ['acctno','product','curbal','ledgbal','prodcd','amtind','intpaybl','branch'])
if not isavg.is_empty() and 'prodcd' in isavg.columns:
    isavg = isavg.rename({'prodcd': 'progcd'})

print("  Loading MNI CURN124...")
curn = read_sas(mni_path / "curn124.sas7bdat", ['acctno','product','curbal','ledgbal','prodcd','amtind','intpaybl','branch'])
if not curn.is_empty() and 'prodcd' in curn.columns:
    curn = curn.rename({'prodcd': 'progcd'})

print("  Loading IMNI CURN124...")
icurn = read_sas(imni_path / "curn124.sas7bdat", ['acctno','product','curbal','ledgbal','prodcd','amtind','intpaybl','branch'])
if not icurn.is_empty() and 'prodcd' in icurn.columns:
    icurn = icurn.rename({'prodcd': 'progcd'})

print("  Loading MNI FDMTHLY...")
fdm = read_sas(mni_path / "fdmthly.sas7bdat", ['acctno','branch','intplan','curbal','bic','amtind','intpay'])
if not fdm.is_empty():
    fdm = fdm.rename({'bic': 'progcd', 'intplan': 'product', 'intpay': 'intpaybl'})
    fdm = fdm.with_columns(pl.col('curbal').alias('ledgbal'))

print("  Loading IMNI FDMTHLY...")
ifdm = read_sas(imni_path / "fdmthly.sas7bdat", ['acctno','branch','intplan','curbal','bic','amtind','intpay'])
if not ifdm.is_empty():
    ifdm = ifdm.rename({'bic': 'progcd', 'intplan': 'product', 'intpay': 'intpaybl'})
    ifdm = ifdm.with_columns(pl.col('curbal').alias('ledgbal'))

# Check if any data was loaded
if all([df.is_empty() for df in [savg, isavg, curn, icurn, fdm, ifdm]]):
    print("\nERROR: No data loaded from any source!")
    exit(1)

# Filter CURN (remove PRODUCT=139)
if not curn.is_empty() and 'product' in curn.columns:
    curn = curn.filter(pl.col('product') != 139)
    print(f"  MNI CURN filtered: {curn.height:,} records")

if not icurn.is_empty() and 'product' in icurn.columns:
    icurn = icurn.filter(pl.col('product') != 139)
    print(f"  IMNI CURN filtered: {icurn.height:,} records")

# Combine all datasets
datasets = []
for df in [savg, isavg, curn, icurn, fdm, ifdm]:
    if not df.is_empty():
        standardized = standardize(df)
        if not standardized.is_empty():
            datasets.append(standardized)

if not datasets:
    print("\nERROR: No valid datasets to combine!")
    exit(1)

deposit = pl.concat(datasets, how="diagonal_relaxed")
print(f"\n  Combined DEPOSIT: {deposit.height:,} records")

# ============================================
# STEP 2: Apply Filters (Matching SAS exactly)
# ============================================
print("\n[STEP 2] Applying filters...")

# Convert data types for filtering
if 'progcd' in deposit.columns:
    deposit = deposit.with_columns(pl.col('progcd').cast(pl.Utf8))
if 'product' in deposit.columns:
    deposit = deposit.with_columns(pl.col('product').cast(pl.Float64))
if 'intpaybl' in deposit.columns:
    deposit = deposit.with_columns(pl.col('intpaybl').cast(pl.Float64))

# SAS: IF PRODCD IN (...)
deposit = deposit.filter(pl.col('progcd').is_in(VALID_PROGCD))
print(f"  After PROGCD filter: {deposit.height:,}")

# SAS: IF PRODUCT = 166 THEN PRODCD = '42310'
deposit = deposit.with_columns([
    pl.when(pl.col('product') == 166)
    .then(pl.lit('42310'))
    .otherwise(pl.col('progcd'))
    .alias('progcd')
])
print(f"  After PRODUCT=166: {deposit.height:,}")

# SAS: IF PRODCD IN ('42199','42699') AND PRODUCT NOT IN (72,413) THEN DELETE
deposit = deposit.filter(
    ~(pl.col('progcd').is_in(['42199','42699']) & ~pl.col('product').is_in([72.0, 413.0]))
)
print(f"  After PROGCD special: {deposit.height:,}")

# SAS: IF PRODUCT IN (30,31,32,33,34) THEN DELETE
deposit = deposit.filter(~pl.col('product').is_in([30.0, 31.0, 32.0, 33.0, 34.0]))
print(f"  After PRODUCT filter: {deposit.height:,}")

# SAS: IF INTPAYBL < 0 THEN INTPAYBL = 0
deposit = deposit.with_columns([
    pl.when(pl.col('intpaybl') < 0)
    .then(0)
    .otherwise(pl.col('intpaybl'))
    .alias('intpaybl')
])
print(f"  After INTPAYBL: {deposit.height:,}")

# Deduplicate by ACCTNO (keep first occurrence)
if not deposit.is_empty():
    deposit = deposit.unique(subset=['acctno'], keep='first')
    deposit.write_parquet(output_path / "DEPOSIT.parquet")
    print(f"\n  DEPOSIT saved: {deposit.height:,} records")
else:
    print("\n  WARNING: DEPOSIT is empty after all filters!")
    exit(1)

# ============================================
# STEP 3: Load and Aggregate FLOAT
# ============================================
print("\n[STEP 3] Loading FLOAT data...")

float_df = read_sas(pidms_path / "float.sas7bdat")
if not float_df.is_empty():
    if 'float' in float_df.columns:
        float_df = float_df.with_columns(pl.col('float').cast(pl.Float64))
    if 'acctno' in float_df.columns:
        float_df = float_df.with_columns(pl.col('acctno').cast(pl.Int64))
    
    float_summary = float_df.group_by('acctno').agg([
        pl.col('float').sum().alias('float')
    ])
    float_summary.write_parquet(output_path / "FLOAT_SUMMARY.parquet")
    print(f"  FLOAT loaded: {float_summary.height:,} unique accounts")
else:
    float_summary = pl.DataFrame()
    print("  WARNING: No FLOAT data found!")

# ============================================
# STEP 4: Find B AND NOT A
# ============================================
print("\n[STEP 4] Finding B AND NOT A...")

if not deposit.is_empty() and not float_summary.is_empty():
    deposit_acctnos = set(deposit['acctno'].unique().to_list())
    float_acctnos = set(float_summary['acctno'].unique().to_list())
    
    print(f"  DEPOSIT unique ACCTNOs: {len(deposit_acctnos):,}")
    print(f"  FLOAT unique ACCTNOs: {len(float_acctnos):,}")
    
    overlap = deposit_acctnos & float_acctnos
    print(f"  Overlap: {len(overlap):,}")
    
    float_only = float_summary.filter(~pl.col('acctno').is_in(list(deposit_acctnos)))
    print(f"  FLOAT_ONLY (B AND NOT A): {float_only.height:,} records")
    
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
        
        float_only_output.write_parquet(output_path / "FLOAT_ONLY.parquet")
        float_only_output.write_csv(output_path / "FLOAT_ONLY.csv")
        generate_report(float_only_output, output_path)
        
        print(f"\n  Total FLOAT amount: {float_only['float'].sum():,.2f}")
    else:
        print("\n  No FLOAT_ONLY records found")
        generate_report(pl.DataFrame(), output_path)
else:
    print("  No DEPOSIT or FLOAT data to process")
    generate_report(pl.DataFrame(), output_path)

print("\n" + "="*60)
print("PROCESSING COMPLETED SUCCESSFULLY")
print("="*60)
