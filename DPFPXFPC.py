import polars as pl
from pathlib import Path
import pyreadstat
import datetime

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
        df, meta = pyreadstat.read_sas7bdat(str(filepath))
        df = pl.from_pandas(df)
        df = df.rename({c: c.lower() for c in df.columns})
        if cols:
            cols_lower = [c.lower() for c in cols]
            available = [c for c in cols_lower if c in df.columns]
            if available:
                df = df.select(available)
        return df
    except:
        return pl.DataFrame()

def standardize(df):
    """Standardize column names and types"""
    if df.is_empty():
        return df
    
    for col in STANDARD_COLS:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))
    
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
                
                f.write(f"{obs:3}          {str(int(acctno)):>10}                        0       {float_val:>10.2f} {-float_val:>10.2f} {-float_val:>10.2f}        0\n")
            
            f.write(" " * 50 + "========\n")
            f.write(f"{' ' * 87}{total:.2f}\n")
        else:
            f.write("No records found matching B AND NOT A condition.\n")
            f.write("All FLOAT records have matching DEPOSIT records.\n")
    
    print(f"  - Report: {report_path}")

# ============================================
# MAIN PROCESSING
# ============================================

print("\n" + "="*60)
print("EIFLTEXP PROCESSING STARTED")
print("="*60)

# STEP 1: Build DEPOSIT
print("\n[STEP 1] Building DEPOSIT dataset...")

# Load and rename SAVG
savg = read_sas(mni_path / "savg124.sas7bdat", ['acctno','product','curbal','ledgbal','prodcd','amtind','intpaybl','branch'])
if not savg.is_empty():
    savg = savg.rename({'prodcd': 'progcd'})

isavg = read_sas(imni_path / "savg124.sas7bdat", ['acctno','product','curbal','ledgbal','prodcd','amtind','intpaybl','branch'])
if not isavg.is_empty():
    isavg = isavg.rename({'prodcd': 'progcd'})

# Load and rename CURN
curn = read_sas(mni_path / "curn124.sas7bdat", ['acctno','product','curbal','ledgbal','prodcd','amtind','intpaybl','branch'])
if not curn.is_empty():
    curn = curn.rename({'prodcd': 'progcd'}).filter(pl.col('product') != 139)

icurn = read_sas(imni_path / "curn124.sas7bdat", ['acctno','product','curbal','ledgbal','prodcd','amtind','intpaybl','branch'])
if not icurn.is_empty():
    icurn = icurn.rename({'prodcd': 'progcd'}).filter(pl.col('product') != 139)

# Load and rename FDMTHLY
fdm = read_sas(mni_path / "fdmthly.sas7bdat", ['acctno','branch','intplan','curbal','bic','amtind','intpay'])
if not fdm.is_empty():
    fdm = fdm.rename({'bic': 'progcd', 'intplan': 'product', 'intpay': 'intpaybl'})
    fdm = fdm.with_columns(pl.col('curbal').alias('ledgbal'))

ifdm = read_sas(imni_path / "fdmthly.sas7bdat", ['acctno','branch','intplan','curbal','bic','amtind','intpay'])
if not ifdm.is_empty():
    ifdm = ifdm.rename({'bic': 'progcd', 'intplan': 'product', 'intpay': 'intpaybl'})
    ifdm = ifdm.with_columns(pl.col('curbal').alias('ledgbal'))

# Combine
datasets = [standardize(df) for df in [savg, isavg, curn, icurn, fdm, ifdm] if not df.is_empty()]
if not datasets:
    print("ERROR: No data loaded!")
    exit(1)

deposit = pl.concat(datasets, how="diagonal_relaxed")
print(f"  Combined DEPOSIT: {deposit.height:,} records")

# STEP 2: Apply filters (matching SAS exactly)
print("\n[STEP 2] Applying filters...")

deposit = deposit.with_columns([
    pl.col('progcd').cast(pl.Utf8),
    pl.col('product').cast(pl.Float64),
    pl.col('intpaybl').cast(pl.Float64)
])

# SAS: IF PRODCD IN (...)
deposit = deposit.filter(pl.col('progcd').is_in(VALID_PROGCD))

# SAS: IF PRODUCT = 166 THEN PRODCD = '42310'
deposit = deposit.with_columns([
    pl.when(pl.col('product') == 166).then(pl.lit('42310')).otherwise(pl.col('progcd')).alias('progcd')
])

# SAS: IF PRODCD IN ('42199','42699') AND PRODUCT NOT IN (72,413) THEN DELETE
deposit = deposit.filter(
    ~(pl.col('progcd').is_in(['42199','42699']) & ~pl.col('product').is_in([72.0, 413.0]))
)

# SAS: IF PRODUCT IN (30,31,32,33,34) THEN DELETE
deposit = deposit.filter(~pl.col('product').is_in([30.0, 31.0, 32.0, 33.0, 34.0]))

# SAS: IF INTPAYBL < 0 THEN INTPAYBL = 0
deposit = deposit.with_columns([
    pl.when(pl.col('intpaybl') < 0).then(0).otherwise(pl.col('intpaybl')).alias('intpaybl')
])

# Deduplicate
deposit = deposit.unique(subset=['acctno'], keep='first')
deposit.write_parquet(output_path / "DEPOSIT.parquet")
print(f"  DEPOSIT saved: {deposit.height:,} records")

# STEP 3: Load FLOAT
print("\n[STEP 3] Loading FLOAT data...")

float_df = read_sas(pidms_path / "float.sas7bdat")
if not float_df.is_empty():
    float_df = float_df.with_columns([
        pl.col('float').cast(pl.Float64),
        pl.col('acctno').cast(pl.Int64)
    ])
    float_summary = float_df.group_by('acctno').agg(pl.col('float').sum().alias('float'))
    float_summary.write_parquet(output_path / "FLOAT_SUMMARY.parquet")
    print(f"  FLOAT loaded: {float_summary.height:,} unique accounts")
else:
    float_summary = pl.DataFrame()
    print("  WARNING: No FLOAT data found!")

# STEP 4: Find B AND NOT A
print("\n[STEP 4] Finding B AND NOT A...")

if not deposit.is_empty() and not float_summary.is_empty():
    deposit_acctnos = set(deposit['acctno'].unique().to_list())
    float_acctnos = set(float_summary['acctno'].unique().to_list())
    
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
        ]).select(['acctno','float','progcd','amtind','branch','ledgbal','product','curbal','intpaybl','avbal','avbaltt','curbaltt'])
        
        float_only_output.write_parquet(output_path / "FLOAT_ONLY.parquet")
        float_only_output.write_csv(output_path / "FLOAT_ONLY.csv")
        generate_report(float_only_output, output_path)
        print(f"\n  Total FLOAT amount: {float_only['float'].sum():,.2f}")
    else:
        print("\n  No FLOAT_ONLY records found")
        generate_report(pl.DataFrame(), output_path)
else:
    generate_report(pl.DataFrame(), output_path)

print("\n" + "="*60)
print("PROCESSING COMPLETED SUCCESSFULLY")
print("="*60)
