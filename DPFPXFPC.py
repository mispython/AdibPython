import polars as pl
from pathlib import Path
import pyreadstat

# Configuration
mni_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/MNI")
imni_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/IMNI")
pidms_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/PIDM")
output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBFLOAT")
output_path.mkdir(exist_ok=True)

def read_sas(file_path):
    """Read SAS file and return polars DataFrame with lowercase columns"""
    try:
        df, _ = pyreadstat.read_sas7bdat(str(file_path))
        return pl.from_pandas(df).rename({c: c.lower() for c in df.columns})
    except Exception as e:
        print(f"NOTE: {file_path.name} not found or error: {e}")
        return pl.DataFrame()

def write_output(df, name, text_file=None):
    """Write DataFrame to Parquet and optionally text file"""
    if not df.is_empty():
        df.write_parquet(output_path / f"{name}.parquet")
        if text_file:
            # Select required columns and write with delimiter
            output_df = df.select(['acctno', 'branch', 'float'])
            with open(output_path / f"{text_file}.txt", "w") as f:
                f.write("ACCTNO\x05BRANCH\x05FLOAT\x05\n")
                for row in output_df.iter_rows():
                    f.write(f"{row[0]}\x05{row[1]}\x05{row[2]}\x05\n")
            print(f"{text_file}.txt created")

def process_float_data(source_path, label):
    """Generic function to process conventional or Islamic float data"""
    print(f"\nPROCESSING {label} BANKING FLOAT DATA")
    print("=" * 50)
    
    # Read and process FDMTHLY
    fdmthly = read_sas(source_path / "fdmthly.sas7bdat")
    if not fdmthly.is_empty():
        fdmthly = fdmthly.select(['acctno', 'branch', 'curbal', 'intplan', 'bic', 'amtind', 'intpay']) \
            .with_columns([
                pl.col('curbal').alias('ledgbal'),
                pl.col('intplan').alias('product'),
                pl.col('bic').alias('prodcd'),
                pl.col('intpay').alias('intpaybl')
            ])
    
    # Read and process CURN124
    curn = read_sas(source_path / "curn124.sas7bdat")
    if not curn.is_empty():
        curn = curn.filter(pl.col('product') != 139) \
            .select(['acctno', 'branch', 'curbal', 'prodcd', 'product', 'amtind', 'intpaybl']) \
            .with_columns(pl.col('curbal').alias('ledgbal'))
    
    # Read and process SAVG124
    savg = read_sas(source_path / "savg124.sas7bdat")
    if not savg.is_empty():
        savg = savg.select(['acctno', 'branch', 'curbal', 'prodcd', 'product', 'amtind', 'intpaybl']) \
            .with_columns(pl.col('curbal').alias('ledgbal'))
    
    # Combine datasets
    datasets = [df for df in [savg, curn, fdmthly] if not df.is_empty()]
    if not datasets:
        print(f"No {label} DEPOSIT data created")
        return pl.DataFrame()
    
    # Standardize columns and concatenate
    target_cols = ['acctno', 'branch', 'curbal', 'ledgbal', 'prodcd', 'product', 'amtind', 'intpaybl']
    standardized = []
    for df in datasets:
        for col in target_cols:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))
        standardized.append(df.select(target_cols))
    
    deposit = pl.concat(standardized, how="vertical")
    
    # Apply filters and transformations
    valid_progcd = ['42110', '42310', '42120', '42320', '42130', '42133', '42132', 
                    '42180', '42610', '42630', '34180', '42199', '42699']
    
    deposit = deposit.filter(pl.col('prodcd').is_in(valid_progcd)) \
        .with_columns([
            pl.when(pl.col('product') == 166).then(pl.lit('42310')).otherwise(pl.col('prodcd')).alias('prodcd')
        ]) \
        .filter(
            ~((pl.col('prodcd').is_in(['42199', '42699'])) & (~pl.col('product').is_in([72, 413])))
        ) \
        .filter(~pl.col('product').is_in([30, 31, 32, 33, 34])) \
        .with_columns([
            pl.when(pl.col('intpaybl') < 0).then(0).otherwise(pl.col('intpaybl')).alias('intpaybl')
        ])
    
    print(f"{label} DEPOSIT records: {deposit.height}")
    
    # Read and aggregate FLOAT data
    float_df = read_sas(pidms_path / "float.sas7bdat")
    if float_df.is_empty():
        print("No FLOAT data found")
        return pl.DataFrame()
    
    float_summary = float_df.group_by('acctno').agg(pl.col('float').sum().alias('float'))
    
    # Merge with deposit data
    merged = deposit.sort('acctno').join(float_summary, on='acctno', how='full', suffix='_float')
    
    # Ensure float column exists
    if 'float' not in merged.columns:
        merged = merged.with_columns(pl.lit(0).alias('float'))
    
    # Apply calculations
    processed = merged.with_columns([
        pl.when(pl.col('curbal') < 0).then(0).otherwise(pl.col('curbal')).alias('curbal'),
        pl.col('curbal').alias('floatori'),
        (pl.col('curbal') - pl.col('float')).alias('avbal'),
        (pl.col('curbal') - pl.col('float')).alias('minusfloat')
    ]).with_columns([
        pl.when(pl.col('avbal') < 0)
        .then(pl.struct([pl.col('curbal').alias('float'), pl.lit(0).alias('avbal')]))
        .otherwise(pl.struct([pl.col('float').alias('float'), pl.col('avbal').alias('avbal')]))
        .alias('adj')
    ]).with_columns([
        pl.col('adj').struct.field('float').alias('float'),
        pl.col('adj').struct.field('avbal').alias('avbal'),
        (pl.col('avbal') + pl.col('intpaybl')).alias('avbaltt'),
        (pl.col('curbal') + pl.col('intpaybl')).alias('curbaltt')
    ]).drop('adj')
    
    # Split into final and except datasets
    final = processed.filter(
        pl.col('curbal').is_not_null() & 
        pl.col('product').is_not_null() & 
        pl.col('float').is_not_null()
    )
    except_df = processed.filter(
        pl.col('float').is_not_null() & 
        (pl.col('curbal').is_null() | pl.col('product').is_null())
    )
    
    # Determine output names
    prefix = "I" if label == "ISLAMIC" else ""
    deposit_name = f"{prefix}DEPOSIT_{label}"
    except_name = f"EXCEPT_{label}"
    float_name = f"{prefix}FLOAT"
    
    # Write outputs
    write_output(final, deposit_name, float_name)
    write_output(except_df, except_name)
    
    print(f"{label} DEPOSIT final records: {final.height}")
    print(f"{label} EXCEPT records: {except_df.height}")
    
    return final

# Main execution
if __name__ == "__main__":
    conventional = process_float_data(mni_path, "CONVENTIONAL")
    islamic = process_float_data(imni_path, "ISLAMIC")
    
    print("\n" + "="*80)
    print("PROCESSING COMPLETED SUCCESSFULLY")
    print("="*80)
    print(f"Conventional records: {conventional.height if not conventional.is_empty() else 0}")
    print(f"Islamic records: {islamic.height if not islamic.is_empty() else 0}")
