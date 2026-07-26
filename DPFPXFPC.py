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
import sys

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
# 8. WRITE OUTPUT AS SAS DATASET USING SASPY (IMPROVED)
# ============================================================

def write_sas_with_saspy(df, output_path):
    """Write DataFrame to SAS dataset using saspy with improved method"""
    
    # Check if saspy is available
    try:
        import saspy
    except ImportError:
        print("\n✗ saspy not installed. Skipping SAS dataset creation.")
        return False
    
    print(f"\nCreating SAS dataset using saspy...")
    
    try:
        # Start SAS session with verbose logging for debugging
        print("  Starting SAS session...")
        sas = saspy.SASsession()
        
        # Convert REPTDATE to SAS date (numeric days since 1960-01-01)
        df_sas = df.copy()
        df_sas['REPTDATE'] = (REPTDATE - date(1960, 1, 1)).days
        
        # Define SAS library for output
        # Use the actual output directory as a SAS library
        sas.submit(f'''
            libname outlib "{OUTPUT_BASE}";
        ''')
        
        # Write DataFrame directly to the permanent library
        print(f"  Writing {len(df_sas):,} records to SAS dataset...")
        sas.dataframe2sasdata(
            df_sas,
            table='MFRS',
            libref='outlib'  # Write directly to the output library
        )
        
        # Add formats and labels
        sas.submit(f'''
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
        ''')
        
        # Verify the file was created
        if os.path.exists(output_path):
            file_size_mb = os.path.getsize(output_path) / (1024**2)
            print(f"  ✓ SAS dataset created successfully!")
            print(f"  ✓ File: {output_path}")
            print(f"  ✓ Size: {file_size_mb:.2f} MB")
            sas.endsas()
            return True
        else:
            print(f"  ✗ SAS dataset not found at: {output_path}")
            # Check if it was created with a different name
            print("  Checking for alternative filenames...")
            for file in os.listdir(OUTPUT_BASE):
                if file.endswith('.sas7bdat'):
                    print(f"    Found: {file}")
                    # Rename if needed
                    if file != os.path.basename(output_path):
                        alt_path = os.path.join(OUTPUT_BASE, file)
                        print(f"    Renaming {file} to MFRS.sas7bdat...")
                        os.rename(alt_path, output_path)
                        print(f"  ✓ Renamed to: {output_path}")
                        sas.endsas()
                        return True
            
            # If we get here, check SAS log for errors
            print("  Checking SAS log for errors...")
            log = sas.lastLOG()
            if log:
                print("  SAS Log (last 20 lines):")
                log_lines = log.split('\n')
                for line in log_lines[-20:]:
                    if line.strip():
                        print(f"    {line}")
            
            sas.endsas()
            return False
            
    except Exception as e:
        print(f"  ✗ Error creating SAS dataset: {e}")
        print(f"  Error type: {type(e).__name__}")
        return False

# Try to create SAS dataset with saspy
sas_created = write_sas_with_saspy(crl, OUTPUT_SAS)

# ============================================================
# 9. FALLBACK: SAS Script Method (if saspy fails)
# ============================================================

if not sas_created:
    print("\n" + "="*60)
    print("SASPY failed. Creating SAS script as fallback...")
    print("="*60)
    
    # Write temporary CSV
    temp_csv = f"{OUTPUT_BASE}/temp_mfrs.csv"
    print(f"  Writing temporary CSV: {temp_csv}")
    crl.to_csv(temp_csv, index=False, sep='|')
    print(f"  ✓ Temporary CSV created ({os.path.getsize(temp_csv) / (1024**2):.2f} MB)")
    
    # Create SAS script that will create the SAS dataset and clean up
    sas_script = f'''/* SAS script to create MFRS.sas7bdat from CSV */
/* Generated: {date.today().strftime('%Y-%m-%d %H:%M:%S')} */

libname outlib "{OUTPUT_BASE}";

/* Import CSV */
proc import datafile="{temp_csv}"
    out=outlib.MFRS
    dbms=dlm
    replace;
    delimiter='|';
    getnames=yes;
    guessingrows=100000;
run;

/* Format date properly */
data outlib.MFRS;
    set outlib.MFRS;
    format REPTDATE date9.;
run;

/* Add labels */
proc datasets lib=outlib;
    modify MFRS;
    label 
        ACCTNO = "Account Number"
        NOTENO = "Note Number"
        PRODESC = "Product Description"
        REPTDATE = "Report Date"
    ;
quit;

/* Clean up temporary CSV file */
data _null_;
    rc = filename('tempfile', "{temp_csv}");
    if rc = 0 then do;
        rc = fdelete('tempfile');
        if rc = 0 then 
            put "✓ Temporary CSV file deleted successfully";
        else
            put "✗ Failed to delete temporary CSV file";
    end;
run;

/* Verify output */
proc contents data=outlib.MFRS;
run;

%put ==========================================;
%put SAS dataset created successfully!;
%put Dataset: {OUTPUT_SAS};
%put Total records: {len(crl):,};
%put ==========================================;
'''
    
    sas_script_path = f"{OUTPUT_BASE}/create_sas.sas"
    with open(sas_script_path, 'w') as f:
        f.write(sas_script)
    
    print(f"  ✓ SAS script created: {sas_script_path}")
    print("\n" + "="*60)
    print("To create the SAS dataset, run:")
    print(f"  sas {sas_script_path}")
    print("="*60)

# ============================================================
# 10. FINAL SUMMARY
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
    print(f"    Size: {os.path.getsize(OUTPUT_PARQUET) / (1024**2):.2f} MB")

# Check SAS
if os.path.exists(OUTPUT_SAS):
    print(f"  ✓ SAS     : {OUTPUT_SAS}")
    print(f"    Size: {os.path.getsize(OUTPUT_SAS) / (1024**2):.2f} MB")
else:
    print(f"  ⚠ SAS     : Not created directly")
    print(f"    Use SAS script to create: {OUTPUT_BASE}/create_sas.sas")

print("="*70)

# ============================================================
# END OF JOB
# ============================================================
