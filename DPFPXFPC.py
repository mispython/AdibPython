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

OUTPUT_BASE = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/ioutput/EIBNMMFR"

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_BASE, exist_ok=True)

OUTPUT_SAS   = f"{OUTPUT_BASE}/SAP.MTH.MFRS.BNM01.DTLFTP.sas7bdat"
OUTPUT_PARQUET = f"{OUTPUT_BASE}/SAP.MTH.MFRS.BNM01.DTLFTP.parquet"

# ============================================================
# 4. LOAD SAS DATASETS WITH PYREADSTAT
# ============================================================

def load_sas(path):
    """Load SAS dataset using pyreadstat"""
    df, meta = pyreadstat.read_sas7bdat(path)
    return df

print("Loading SAS datasets...")
pbb_alm_cr   = load_sas(PBB_ALM_CR_PATH)
pbb_mast_br  = load_sas(PBB_MAST_BR_PATH)
pibb_alm_cr  = load_sas(PIBB_ALM_CR_PATH)
pibb_mast_br = load_sas(PIBB_MAST_BR_PATH)

# ============================================================
# 5. FILTER + PREPARE DATA (SAS DATA STEP)
# ============================================================

VALID_PRODESC = [
    "BILLS RETAIL",
    "TOTAL COMMERCIAL RETAILS"
]

def prepare_df(df1, df2):
    """Combine two dataframes, filter, and add REPTDATE"""
    df = pd.concat([df1, df2], ignore_index=True)
    df = df[df["PRODESC"].isin(VALID_PRODESC)].copy()
    df["REPTDATE"] = REPTDATE
    return df

print("Preparing PBB data...")
pbb = prepare_df(pbb_alm_cr, pbb_mast_br)

print("Preparing PIBB data...")
pibb = prepare_df(pibb_alm_cr, pibb_mast_br)

# ============================================================
# 6. COMBINE PBB + PIBB
# ============================================================

print("Combining datasets...")
crl = pd.concat([pbb, pibb], ignore_index=True)

# Keep only required columns
crl = crl[["ACCTNO", "NOTENO", "PRODESC", "REPTDATE"]]

# ============================================================
# 7. WRITE OUTPUT AS SAS DATASET USING SASPY
# ============================================================

def write_sas_with_saspy(df, output_path, repdate):
    """Write DataFrame to SAS dataset using saspy"""
    try:
        import saspy
        print("Using saspy to write SAS dataset...")
        
        # Start SAS session
        sas = saspy.SASsession()
        
        # Convert REPTDATE to SAS date (numeric days since 1960-01-01)
        # and create a copy to avoid modifying original
        df_copy = df.copy()
        df_copy['REPTDATE'] = (repdate - date(1960, 1, 1)).days
        
        # Write to SAS temporary dataset
        sas.dataframe2sasdata(
            df_copy,
            table='DTLFTP_TEMP',
            libref='WORK'
        )
        
        # Use SAS to create permanent dataset with proper name
        # Get just the filename without extension
        sas_filename = os.path.basename(output_path).replace('.sas7bdat', '')
        
        # Submit SAS code to copy to permanent location
        sas_code = f'''
            libname outlib "{OUTPUT_BASE}";
            
            data outlib.{sas_filename};
                set work.DTLFTP_TEMP;
            run;
            
            proc datasets lib=outlib;
                modify {sas_filename};
                label 
                    ACCTNO = "Account Number"
                    NOTENO = "Note Number"
                    PRODESC = "Product Description"
                    REPTDATE = "Report Date"
                ;
            quit;
        '''
        
        # Execute SAS code
        result = sas.submit(sas_code)
        
        # Check for errors
        if 'ERROR' in result['LOG']:
            print("WARNING: SAS log contains errors:")
            print(result['LOG'])
            sas.endsas()
            return False
        
        sas.endsas()
        
        # Verify file was created
        if os.path.exists(output_path):
            print(f"SAS dataset created successfully: {output_path}")
            return True
        else:
            print(f"ERROR: SAS dataset not found at {output_path}")
            return False
            
    except ImportError:
        print("saspy not available. Only Parquet output will be created.")
        print("To install saspy: pip install saspy")
        print("Note: saspy requires SAS installed on the system")
        return False
    except Exception as e:
        print(f"Error writing SAS dataset: {e}")
        return False

# Try to write SAS dataset
sas_success = write_sas_with_saspy(crl, OUTPUT_SAS, REPTDATE)

# ============================================================
# 8. WRITE OUTPUT AS PARQUET
# ============================================================

print(f"\nWriting Parquet output to: {OUTPUT_PARQUET}")
crl.to_parquet(
    OUTPUT_PARQUET,
    engine="pyarrow",
    compression="snappy",
    index=False
)

# ============================================================
# 9. ALTERNATIVE: Write SAS using CSV + SAS script (if saspy fails)
# ============================================================

if not sas_success:
    print("\nAttempting alternative method: CSV + SAS script...")
    
    # Write CSV
    csv_path = f"{OUTPUT_BASE}/temp_dtlfpt.csv"
    crl.to_csv(csv_path, index=False, sep='|')
    
    # Create SAS script
    sas_script = f'''/* SAS script to convert CSV to SAS dataset */
    libname outlib "{OUTPUT_BASE}";
    
    proc import datafile="{csv_path}"
        out=outlib.SAP_MTH_MFRS_BNM01_DTLFTP
        dbms=dlm
        replace;
        delimiter='|';
        getnames=yes;
        guessingrows=10000;
    run;
    
    data outlib.SAP_MTH_MFRS_BNM01_DTLFTP;
        set outlib.SAP_MTH_MFRS_BNM01_DTLFTP;
        format REPTDATE date9.;
        /* REPTDATE is already in SAS date format */
    run;
    
    proc datasets lib=outlib;
        modify SAP_MTH_MFRS_BNM01_DTLFTP;
        label 
            ACCTNO = "Account Number"
            NOTENO = "Note Number"
            PRODESC = "Product Description"
            REPTDATE = "Report Date"
        ;
    quit;
    '''
    
    sas_script_path = f"{OUTPUT_BASE}/convert_csv_to_sas.sas"
    with open(sas_script_path, 'w') as f:
        f.write(sas_script)
    
    print(f"Created SAS conversion script: {sas_script_path}")
    print(f"To create SAS dataset, run: sas {sas_script_path}")
    print(f"Or manually import the CSV file: {csv_path}")

# ============================================================
# 10. PRINT SUMMARY
# ============================================================

print("\n" + "="*60)
print("EIBNMMFR job completed successfully!")
print("="*60)
print(f"Report Date    : {REPTDATE.strftime('%d-%b-%Y')}")
print(f"Total records  : {len(crl):,}")
print(f"Columns        : {', '.join(crl.columns)}")

if os.path.exists(OUTPUT_SAS):
    print(f"✓ SAS output     : {OUTPUT_SAS}")
else:
    print(f"✗ SAS output     : Not created (see alternative method above)")

print(f"✓ Parquet output : {OUTPUT_PARQUET}")
print("="*60)

# ============================================================
# END OF JOB
# ============================================================
