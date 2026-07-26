# ============================================================
# JOB NAME : EIBNMMFR (Python)
# INPUT    : SAS datasets (.sas7bdat)
# OUTPUT   : SAS dataset (.sas7bdat) and Parquet
# PURPOSE  : Replace JCL + SAS job
# ============================================================

import pandas as pd
import pyreadstat
from datetime import date, timedelta
import os
import gc
import subprocess

# ============================================================
# 1. REPORT DATE LOGIC (SAS REPTDATE)
# ============================================================

today = date.today()
first_of_this_month = today.replace(day=1)
REPTDATE = first_of_this_month - timedelta(days=1)

REPTYEAR = REPTDATE.strftime("%y")
REPTMON  = REPTDATE.strftime("%m")
REPTDAY  = REPTDATE.strftime("%d")
RDATE    = REPTDATE.strftime("%d%m%Y")

print(f"Report Date: {REPTDATE.strftime('%d-%b-%Y')}")

# ============================================================
# 2. INPUT SAS DATASET PATHS
# ============================================================

INPUT_BASE = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBNMMFR"

PBB_ALM_CR_PATH   = f"{INPUT_BASE}/pbb_alm_cr.sas7bdat"
PBB_MAST_BR_PATH  = f"{INPUT_BASE}/pbb_mast_br.sas7bdat"
PIBB_ALM_CR_PATH  = f"{INPUT_BASE}/pibb_alm_cr.sas7bdat"
PIBB_MAST_BR_PATH = f"{INPUT_BASE}/pibb_mast_br.sas7bdat"

# ============================================================
# 3. OUTPUT PATHS
# ============================================================

OUTPUT_BASE = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBNMMFR"

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_BASE, exist_ok=True)

OUTPUT_SAS     = f"{OUTPUT_BASE}/MFRS.sas7bdat"
OUTPUT_PARQUET = f"{OUTPUT_BASE}/MFRS.parquet"

# ============================================================
# 4. LOAD SAS DATASETS WITH PYREADSTAT
# ============================================================

def load_sas_dataset(path, dataset_name):
    """Load SAS dataset using pyreadstat"""
    print(f"  Loading {dataset_name}...")
    try:
        df, meta = pyreadstat.read_sas7bdat(path)
        print(f"    ✓ Loaded {len(df):,} records with {len(df.columns)} columns")
        return df
    except Exception as e:
        print(f"    ✗ Error loading {dataset_name}: {e}")
        raise

print("\nLoading SAS datasets...")
pbb_alm_cr   = load_sas_dataset(PBB_ALM_CR_PATH, "PBB_ALM_CR")
pbb_mast_br  = load_sas_dataset(PBB_MAST_BR_PATH, "PBB_MAST_BR")
pibb_alm_cr  = load_sas_dataset(PIBB_ALM_CR_PATH, "PIBB_ALM_CR")
pibb_mast_br = load_sas_dataset(PIBB_MAST_BR_PATH, "PIBB_MAST_BR")

# ============================================================
# 5. FILTER + PREPARE DATA (SAS DATA STEP)
# ============================================================

VALID_PRODESC = [
    "BILLS RETAIL",
    "TOTAL COMMERCIAL RETAILS"
]

def prepare_df(df1, df2, df_name):
    """Combine two dataframes, filter, and add REPTDATE"""
    print(f"\nPreparing {df_name} data...")
    
    # Concatenate
    df = pd.concat([df1, df2], ignore_index=True)
    print(f"  Combined: {len(df):,} records")
    
    # Filter
    df = df[df["PRODESC"].isin(VALID_PRODESC)].copy()
    print(f"  After filter: {len(df):,} records")
    
    # Add REPTDATE
    df["REPTDATE"] = REPTDATE
    
    # Free memory from original dataframes
    del df1, df2
    gc.collect()
    
    return df

pbb = prepare_df(pbb_alm_cr, pbb_mast_br, "PBB")
pibb = prepare_df(pibb_alm_cr, pibb_mast_br, "PIBB")

# ============================================================
# 6. COMBINE PBB + PIBB
# ============================================================

print("\nCombining datasets...")
crl = pd.concat([pbb, pibb], ignore_index=True)
print(f"  Total combined: {len(crl):,} records")

# Keep only required columns
crl = crl[["ACCTNO", "NOTENO", "PRODESC", "REPTDATE"]]
print(f"  Final columns: {', '.join(crl.columns)}")

# Free memory
del pbb, pibb
gc.collect()

# ============================================================
# 7. WRITE OUTPUT AS PARQUET
# ============================================================

print(f"\nWriting Parquet output to: {OUTPUT_PARQUET}")
crl.to_parquet(
    OUTPUT_PARQUET,
    engine="pyarrow",
    compression="snappy",
    index=False
)
print(f"  ✓ Parquet file size: {os.path.getsize(OUTPUT_PARQUET) / (1024**2):.2f} MB")

# ============================================================
# 8. WRITE OUTPUT AS SAS DATASET USING SASPY
# ============================================================

def write_sas_dataset(df, output_path):
    """Write DataFrame to SAS dataset using saspy"""
    try:
        import saspy
        print(f"\nCreating SAS dataset: {output_path}")
        
        # Start SAS session
        sas = saspy.SASsession()
        
        # Convert REPTDATE to SAS date (numeric days since 1960-01-01)
        df_sas = df.copy()
        df_sas['REPTDATE'] = (REPTDATE - date(1960, 1, 1)).days
        
        # Write to SAS WORK library
        sas.dataframe2sasdata(
            df_sas,
            table='MFRS',
            libref='WORK'
        )
        
        # Copy to permanent location with proper libname
        sas.submit(f'''
            libname outlib "{OUTPUT_BASE}";
            
            data outlib.MFRS;
                set work.MFRS;
                format REPTDATE date9.;
            run;
            
            proc datasets lib=outlib;
                modify MFRS;
                label 
                    ACCTNO = "Account Number"
                    NOTENO = "Note Number"
                    PRODESC = "Product Description"
                    REPTDATE = "Report Date"
                ;
            quit;
        ''')
        
        sas.endsas()
        
        # Verify file was created
        if os.path.exists(output_path):
            print(f"  ✓ SAS dataset created: {output_path}")
            print(f"  ✓ SAS file size: {os.path.getsize(output_path) / (1024**2):.2f} MB")
            return True
        else:
            print(f"  ✗ SAS dataset not found at {output_path}")
            return False
            
    except ImportError:
        print("\n✗ saspy not available. SAS dataset will not be created.")
        print("  To install: pip install saspy")
        print("  Note: saspy requires SAS installed on the system")
        return False
    except Exception as e:
        print(f"\n✗ Error creating SAS dataset: {e}")
        return False

# Try to write SAS dataset
sas_success = write_sas_dataset(crl, OUTPUT_SAS)

# ============================================================
# 9. ALTERNATIVE: Use SAS script if saspy fails
# ============================================================

if not sas_success:
    print("\nCreating SAS import script as fallback...")
    
    # Write temporary CSV for SAS import
    temp_csv = f"{OUTPUT_BASE}/temp_mfrs.csv"
    crl.to_csv(temp_csv, index=False, sep='|')
    
    # Create SAS script
    sas_script = f'''/* SAS script to create MFRS.sas7bdat */
libname outlib "{OUTPUT_BASE}";

proc import datafile="{temp_csv}"
    out=outlib.MFRS
    dbms=dlm
    replace;
    delimiter='|';
    getnames=yes;
    guessingrows=100000;
run;

data outlib.MFRS;
    set outlib.MFRS;
    format REPTDATE date9.;
run;

proc datasets lib=outlib;
    modify MFRS;
    label 
        ACCTNO = "Account Number"
        NOTENO = "Note Number"
        PRODESC = "Product Description"
        REPTDATE = "Report Date"
    ;
quit;

/* Clean up temp file */
data _null_;
    rc = filename('tempfile', "{temp_csv}");
    rc = fdelete('tempfile');
run;

%put SAS dataset created successfully!;
%put Total records: {len(crl):,};
'''
    
    sas_script_path = f"{OUTPUT_BASE}/create_sas.sas"
    with open(sas_script_path, 'w') as f:
        f.write(sas_script)
    
    print(f"  ✓ SAS script created: {sas_script_path}")
    print(f"\nTo create SAS dataset, run:")
    print(f"  sas {sas_script_path}")
    print(f"\nOr manually run the SAS script to convert to .sas7bdat")

# ============================================================
# 10. PRINT SUMMARY
# ============================================================

print("\n" + "="*70)
print("EIBNMMFR job completed successfully!")
print("="*70)
print(f"Report Date    : {REPTDATE.strftime('%d-%b-%Y')}")
print(f"Total records  : {len(crl):,}")
print(f"Columns        : {', '.join(crl.columns)}")
print("-"*70)
print("Output files:")

# Check Parquet
if os.path.exists(OUTPUT_PARQUET):
    print(f"  ✓ Parquet : {OUTPUT_PARQUET}")
else:
    print(f"  ✗ Parquet : Not created")

# Check SAS
if os.path.exists(OUTPUT_SAS):
    print(f"  ✓ SAS     : {OUTPUT_SAS}")
else:
    print(f"  ⚠ SAS     : Not created (use SAS script to create)")

print("="*70)

# ============================================================
# END OF JOB
# ============================================================
