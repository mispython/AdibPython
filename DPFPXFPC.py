import pyreadstat
import polars as pl
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np
import sys

# Import PBBBTFMT functions
sys.path.append('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS')
from PBBBTFMT import (
    dayr_format, 
    dirct_format, 
    liab_format, 
    btfcept_format, 
    prctype_format, 
    prctypesfs_format, 
    nsrsliab_format
)

# Import PBBLNFMT functions
from PBBLNFMT import (
    put,
    informat,
    apply_format,
    available_formats
)

# =========================================================
# 1. CONFIGURATION
# =========================================================
BASE_INPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIWBTCR")
BASE_OUTPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIWBTCR")
BASE_OUTPUT.mkdir(parents=True, exist_ok=True)

# =========================================================
# 2. DATE LOGIC
# =========================================================
TDATE = datetime.now() - timedelta(days=1)
day_val = TDATE.day
month_val = TDATE.month
year_val = TDATE.year

if day_val == 8:
    SDD, WK, WK1 = 1, '1', '4'
elif day_val == 15:
    SDD, WK, WK1 = 9, '2', '1'
elif day_val == 22:
    SDD, WK, WK1 = 16, '3', '2'
else:
    SDD, WK, WK1 = 23, '4', '3'

MM = month_val
MM1 = MM
if WK == '1':
    MM1 = MM - 1
    if MM1 == 0:
        MM1 = 12

REPTMON = f"{MM:02d}"
REPTMON1 = f"{MM1:02d}"
REPTYEAR = str(year_val)
REPTYEA2 = str(year_val)[2:]
REPTDAY = f"{day_val:02d}"
RDATE = int(TDATE.strftime("%d%m%y"))
RDATE2 = (TDATE - timedelta(days=1)).strftime("%y%m%d")

# =========================================================
# 3. HELPER FUNCTIONS
# =========================================================
def read_sas(file_path):
    """Read SAS7BDAT file and convert to Polars DataFrame"""
    try:
        df, meta = pyreadstat.read_sas7bdat(str(file_path))
        df.columns = [col.lower() for col in df.columns]
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).replace('nan', '')
                df[col] = df[col].replace('None', '')
        return pl.from_pandas(df)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

def write_fixed_width(df, filepath, columns_spec):
    """Write DataFrame as fixed-width text file"""
    if df is None or df.is_empty():
        print(f"Warning: No data to write for {filepath}")
        return
    
    with open(filepath, 'w') as f:
        for row in df.iter_rows(named=True):
            line = ""
            for col_name, width, format_type in columns_spec:
                value = row.get(col_name, "")
                
                if format_type == 'Z':
                    if value is None or value == '' or pd.isna(value):
                        line += '0' * width
                    else:
                        try:
                            line += f"{int(float(value)):0{width}d}"
                        except:
                            line += '0' * width
                elif format_type == 'S':
                    if value is None or pd.isna(value):
                        value = ''
                    line += f"{str(value):<{width}}"
                elif format_type == 'D':
                    if value is None or value == '' or pd.isna(value):
                        line += ' ' * width
                    else:
                        try:
                            line += f"{float(value):{width}.2f}"
                        except:
                            line += ' ' * width
                elif format_type == 'I':
                    if value is None or value == '' or pd.isna(value):
                        line += ' ' * width
                    else:
                        try:
                            line += f"{int(float(value)):{width}d}"
                        except:
                            line += ' ' * width
                else:
                    if value is None or pd.isna(value):
                        value = ''
                    line += f"{str(value):<{width}}"
            
            f.write(line + "\n")

# =========================================================
# 4. READ INPUT FILES
# =========================================================
print("Reading input files...")

input_files = {
    'imast': BASE_INPUT / f"imast{REPTDAY}{REPTMON}.sas7bdat",
    'imast2': BASE_INPUT / f"imast2{REPTDAY}{REPTMON}.sas7bdat",
    'icred': BASE_INPUT / f"icred{REPTDAY}{REPTMON}.sas7bdat",
    'isuba': BASE_INPUT / f"isuba{REPTDAY}{REPTMON}.sas7bdat",
    'iprov': BASE_INPUT / f"iprov{REPTDAY}{REPTMON}.sas7bdat",
    'iamsubacc': BASE_INPUT / f"iamsubacc{REPTDAY}{REPTMON}.sas7bdat",
    'ibtrad': BASE_INPUT / f"ibtrad{REPTMON}{WK}.sas7bdat",
    'ibtdtl': BASE_INPUT / f"ibtdtl{REPTYEA2}{REPTMON}{REPTDAY}.sas7bdat",
    'lnacct': BASE_INPUT / "lnacct.sas7bdat"
}

data = {}
for key, file_path in input_files.items():
    if file_path.exists():
        data[key] = read_sas(file_path)
        if data[key] is not None:
            print(f"  Read {key}: {data[key].height} rows")
    else:
        print(f"  Missing {key}: {file_path}")
        data[key] = None

# =========================================================
# 5. PROCESS MAST (using PBBLNFMT formats)
# =========================================================
print("\nProcessing MAST...")

mast = None
if data.get('imast') is not None:
    # Step 1: Filter data
    mast = data['imast'].filter(
        pl.col("acctnox").cast(pl.Float64, strict=False) > 2500000000
    )
    
    # Step 2: Add basic columns
    mast = mast.with_columns([
        pl.col("ficode").alias("branch"),
        pl.lit(0).cast(pl.Int64).alias("oldbrh"),
        pl.lit("     ").alias("ficody")
    ])
    
    # Step 3: Add APCODE
    mast = mast.with_columns([
        pl.when(pl.col("retailid") == 'C')
          .then(999)
          .otherwise(0)
          .cast(pl.Int64)
          .alias("apcode")
    ])
    
    # Step 4: Create CUSTCODE_CLEAN
    mast = mast.with_columns([
        pl.when(
            (pl.col("custcodx").is_null()) | (pl.col("custcodx") == "") | (pl.col("custcodx") == "nan")
        ).then(pl.lit("99")).otherwise(pl.col("custcodx")).alias("custcode_clean")
    ])
    
    # Step 5: Apply $LOCUSTCD format
    mast = mast.with_columns([
        pl.col("custcode_clean").cast(pl.Float64, strict=False).map_elements(
            lambda x: put(x, "LOCUSTCD", "99"), return_dtype=pl.Utf8
        ).alias("custfiss")
    ])
    
    # Step 6: Create SECTOR_CLEAN
    mast = mast.with_columns([
        pl.when(
            (pl.col("sector").is_null()) | (pl.col("sector") == "") | (pl.col("sector") == "nan")
        ).then(pl.lit("9999")).otherwise(pl.col("sector")).alias("sector_clean")
    ])
    
    # Step 7: Apply $RVRSE format
    mast = mast.with_columns([
        pl.col("sector_clean").map_elements(
            lambda x: put(x, "RVRSE", "9999"), return_dtype=pl.Utf8
        ).alias("sectfiss")
    ])
    
    # Step 8: Apply $INDSECT if industrial_sector_cd exists
    if 'industrial_sector_cd' in mast.columns:
        mast = mast.with_columns([
            pl.when(
                (pl.col("industrial_sector_cd").is_not_null()) & 
                (pl.col("industrial_sector_cd").str.len_chars() == 5) &
                (pl.col("industrial_sector_cd") != "")
            ).then(
                pl.col("industrial_sector_cd").map_elements(
                    lambda x: put(x, "INDSECT", None), return_dtype=pl.Utf8
                )
            ).otherwise(pl.col("sectfiss")).alias("sectfiss")
        ])
    
    # Step 9: Apply special sector override
    mast = mast.with_columns([
        pl.when(
            pl.col("custfiss").is_in(['77', '78', '95', '96'])
        ).then(pl.lit("9700")).otherwise(pl.col("sectfiss")).alias("sectfiss")
    ])
    
    # Step 10: Remove duplicates
    mast = mast.unique(subset=["acctnox"], keep="first")
    print(f"MAST processed: {mast.height} rows")

# =========================================================
# 6. PROCESS MAST2
# =========================================================
print("\nProcessing MAST2...")

mast2_agg = None
mast2c = None

if data.get('imast2') is not None:
    mast2_df = data['imast2']
    
    if 'aano' in mast2_df.columns:
        mast2_filtered = mast2_df.filter(
            (pl.col("aano").cast(pl.Utf8).str.slice(0, 1) != "") &
            (pl.col("aano").cast(pl.Utf8).str.len_chars() == 13)
        )
        
        if not mast2_filtered.is_empty():
            mast2_agg = mast2_filtered.group_by("acctnox").agg([
                pl.col("aano").cast(pl.Utf8).str.join("|").alias("allrefno"),
                pl.col("apvdate").filter(pl.col("apvdate") > 0).min().alias("firstdisbdt")
            ])
    
    if 'facno' in mast2_df.columns and 'ccpt_ltst_review_dt' in mast2_df.columns:
        mast2c = mast2_df.select([
            pl.col("acctnox"),
            pl.col("facno").cast(pl.Utf8).str.zfill(3).alias("facline"),
            pl.col("ccpt_ltst_review_dt")
        ]).unique(subset=["acctnox", "facline"])
    
    print(f"MAST2 processed")

# =========================================================
# 7. PROCESS CRED
# =========================================================
print("\nProcessing CRED...")

cred = None
if data.get('icred') is not None:
    cred = data['icred'].filter(
        (pl.col("acctnox").cast(pl.Float64, strict=False) > 2500000000) &
        (pl.col("acctnox").cast(pl.Float64, strict=False) != 2501900811) &
        (pl.col("transref").cast(pl.Utf8).str.strip_chars() != "") &
        (pl.col("outstand").cast(pl.Float64, strict=False) >= 0)
    )
    
    # Calculate MATUREDS
    cred = cred.with_columns([
        pl.when(
            (pl.col("maturedx").cast(pl.Utf8) == "000000") | 
            (pl.col("maturedx").cast(pl.Utf8).str.strip_chars() == "")
        ).then(pl.lit(99999)).otherwise(
            pl.col("maturedx").cast(pl.Utf8).str.strptime(pl.Date, "%y%m%d").cast(pl.Int64)
        ).alias("matureds")
    ])
    
    # Calculate NODAYS
    cred = cred.with_columns([
        pl.when(
            (pl.col("matureds") > 0) & (pl.col("matureds") <= RDATE)
        ).then(
            pl.lit(RDATE) - pl.col("matureds") + 1
        ).otherwise(pl.lit(0)).alias("nodays")
    ])
    
    # Calculate ARREARS
    cred = cred.with_columns([
        pl.when(
            (pl.col("matureds") > 0) & 
            (pl.col("matureds") <= RDATE) &
            ((pl.lit(RDATE) - pl.col("matureds") + 1) > 0)
        ).then(
            pl.col("nodays").map_elements(
                lambda x: dayr_format(x), return_dtype=pl.Int64
            )
        ).otherwise(pl.lit(0)).alias("arrears"),
        
        pl.when(pl.col("nodays") > 0).then(1).otherwise(0).alias("instalm"),
        
        pl.col("transref").cast(pl.Utf8).str.slice(0, 7).alias("transrex"),
        pl.col("outstand").cast(pl.Float64, strict=False).alias("outstandx")
    ])
    
    # Ensure acctnox is string for consistent joins
    cred = cred.with_columns([
        pl.col("acctnox").cast(pl.Utf8).alias("acctnox")
    ])
    
    cred = cred.unique(subset=["acctnox", "transref"])
    print(f"CRED processed: {cred.height} rows")

# =========================================================
# 8. PROCESS BNM TRADE DATA
# =========================================================
print("\nProcessing BNM Trade data...")

if data.get('ibtrad') is not None and cred is not None:
    # BTRAD - Balance data (ensure acctnox is string)
    btrad = data['ibtrad'].filter(
        pl.col("balance").cast(pl.Float64, strict=False) > 0
    ).select([
        pl.col("acctnox").cast(pl.Utf8).alias("acctnox"),
        pl.col("transref").cast(pl.Utf8).str.slice(0, 7).alias("transrex"),
        pl.col("balance").cast(pl.Float64, strict=False),
        pl.col("intrecv").cast(pl.Float64, strict=False),
        pl.col("unearned").cast(pl.Float64, strict=False),
        pl.col("liabcode"),
        pl.col("utrdf")
    ])
    
    # BTRAX - Repaid/Disburse (ensure acctnox is string)
    btrax = data['ibtrad'].select([
        pl.col("acctnox").cast(pl.Utf8).alias("acctnox"),
        pl.col("transref").cast(pl.Utf8).str.slice(0, 7).alias("transrex"),
        pl.col("repaid").cast(pl.Float64, strict=False),
        pl.col("disburse").cast(pl.Float64, strict=False),
        pl.col("mtd_tawidh_amt").cast(pl.Float64, strict=False),
        pl.col("mtd_gharamah_amt").cast(pl.Float64, strict=False)
    ])
    
    # INTRT - Interest rates (ensure acctnox is string)
    if data.get('ibtdtl') is not None:
        intrt = data['ibtdtl'].select([
            pl.col("acctnox").cast(pl.Utf8).alias("acctnox"),
            pl.col("transref").cast(pl.Utf8).str.slice(0, 7).alias("transrex"),
            pl.col("intrate").cast(pl.Float64, strict=False),
            pl.col("commrate").cast(pl.Float64, strict=False),
            pl.col("discrate").cast(pl.Float64, strict=False),
            pl.col("combrate").cast(pl.Float64, strict=False),
            pl.col("prinamt_myrx").cast(pl.Float64, strict=False),
            pl.col("intamt_myrx").cast(pl.Float64, strict=False),
            pl.col("oth_chargex").cast(pl.Float64, strict=False),
            pl.col("prodgrp")
        ])
        btrax = btrax.join(intrt, on=["acctnox", "transrex"], how="left")
    
    # Merge with CRED
    cred = cred.join(btrad, on=["acctnox", "transrex"], how="left")
    cred = cred.join(btrax, on=["acctnox", "transrex"], how="left")
    
    # Update OUTSTAND
    cred = cred.with_columns([
        pl.when(
            (pl.col("balance").is_not_null()) & (pl.col("balance") > 0)
        ).then(pl.col("balance")).otherwise(0).alias("outstand"),
        pl.col("unearned").fill_null(0),
        pl.col("repaid").fill_null(0),
        pl.col("disburse").fill_null(0),
        pl.col("mtd_tawidh_amt").fill_null(0),
        pl.col("mtd_gharamah_amt").fill_null(0)
    ])
    
    print(f"BNM Trade data processed")

# =========================================================
# 9. PROCESS SUBA (using PBBBTFMT formats)
# =========================================================
print("\nProcessing SUBA...")

suba = None
suba_main = None
suba9 = None

if data.get('isuba') is not None:
    suba = data['isuba'].filter(
        (pl.col("acctnox").cast(pl.Float64, strict=False) > 2500000000) &
        (pl.col("acctnox").cast(pl.Float64, strict=False) != 2501900811)
    )
    
    # Ensure acctnox is string
    suba = suba.with_columns([
        pl.col("acctnox").cast(pl.Utf8).alias("acctnox")
    ])
    
    # Apply PBBBTFMT format functions
    suba = suba.with_columns([
        # Currency determination
        pl.when(
            ~pl.col("liabcode").cast(pl.Utf8).is_in(["FFS", "FFU", "FCS", "FCU", "FFL", "FTI", "FTL"])
        ).then(pl.lit("MYR")).otherwise(pl.lit(None)).alias("forcurr"),
        
        # AANO extraction
        pl.col("tfdesc01").cast(pl.Utf8).str.slice(0, 13).alias("aano"),
        
        # Apply $LIAB format from PBBBTFMT
        pl.col("liabcode").cast(pl.Utf8).map_elements(
            lambda x: liab_format(x), return_dtype=pl.Utf8
        ).alias("facility"),
        
        # Apply $NSRSLIAB format from PBBBTFMT
        pl.col("liabcode").cast(pl.Utf8).map_elements(
            lambda x: nsrsliab_format(x), return_dtype=pl.Utf8
        ).alias("faccode"),
        
        # Apply $PRCTYPE format from PBBBTFMT
        pl.col("liabcode").cast(pl.Utf8).map_elements(
            lambda x: prctype_format(x), return_dtype=pl.Utf8
        ).alias("typeprc"),
        
        # Apply $PRCTYPESFS format from PBBBTFMT
        pl.col("liabcode").cast(pl.Utf8).map_elements(
            lambda x: prctypesfs_format(x), return_dtype=pl.Utf8
        ).alias("typeprc_sfs"),
        
        # Apply $BTFCEPT format from PBBBTFMT
        pl.col("liabcode").cast(pl.Utf8).map_elements(
            lambda x: btfcept_format(x), return_dtype=pl.Utf8
        ).alias("fconcept")
    ])
    
    # Split into SUBA9 and SUBA main
    suba9 = suba.filter(
        (pl.col("subacct").cast(pl.Utf8) == "OV") & 
        (pl.col("transref").cast(pl.Utf8).str.strip_chars() == "")
    ).unique(subset=["acctnox"], keep="first")
    
    suba_main = suba.filter(
        pl.col("transref").cast(pl.Utf8).str.strip_chars() != ""
    )
    
    print(f"SUBA processed: {suba.height} rows (SUBA9: {suba9.height}, SUBA_MAIN: {suba_main.height})")

# =========================================================
# 10. PROCESS ACCT
# =========================================================
print("\nProcessing ACCT...")

acct = None
if mast is not None and suba9 is not None:
    # Ensure acctnox is string in mast
    mast = mast.with_columns([
        pl.col("acctnox").cast(pl.Utf8).alias("acctnox")
    ])
    
    if mast2c is not None:
        mast2c = mast2c.with_columns([
            pl.col("acctnox").cast(pl.Utf8).alias("acctnox")
        ])
        suba9 = suba9.join(mast2c, on="acctnox", how="left")
    
    acct = mast.join(suba9, on="acctnox", how="inner")
    
    if mast2_agg is not None:
        mast2_agg = mast2_agg.with_columns([
            pl.col("acctnox").cast(pl.Utf8).alias("acctnox")
        ])
        acct = acct.join(mast2_agg, on="acctnox", how="left")
    
    # Apply date calculations
    acct = acct.with_columns([
        pl.lit(20).cast(pl.Int64).alias("issueya"),
        pl.lit(0).cast(pl.Int64).alias("issueyy"),
        pl.lit(0).cast(pl.Int64).alias("issuemm"),
        pl.lit(0).cast(pl.Int64).alias("issuedd"),
        pl.lit(0).cast(pl.Int64).alias("lmtamt"),
        pl.lit(0).cast(pl.Int64).alias("ladtyy"),
        pl.lit(0).cast(pl.Int64).alias("ladtmm"),
        pl.lit(0).cast(pl.Int64).alias("ladtdd"),
        pl.lit(0).cast(pl.Int64).alias("fxrate"),
        pl.lit("     ").alias("climate_prin_taxonomy_class")
    ])
    
    print(f"ACCT processed: {acct.height} rows")

# =========================================================
# 11. PROCESS BTR2
# =========================================================
print("\nProcessing BTR2...")

btr2 = None
btr2x = None
btr3a = None

if cred is not None and suba_main is not None:
    btr2 = cred.join(suba_main, on=["acctnox", "transref"], how="inner")
    
    btr2 = btr2.with_columns([
        # Apply special facility mapping for UTRDF='R'
        pl.when(
            (pl.col("utrdf") == 'R') & (pl.col("liabcode").is_in(['BAE', 'BEI']))
        ).then(pl.lit("34471"))
        .when(
            (pl.col("utrdf") == 'R') & (pl.col("liabcode").is_in(['BAI', 'BII']))
        ).then(pl.lit("34472"))
        .when(
            (pl.col("utrdf") == 'R') & (pl.col("liabcode").is_in(['BAP', 'BAS', 'BPI', 'BSI']))
        ).then(pl.lit("34475"))
        .otherwise(pl.col("facility"))
        .alias("facility"),
        
        # TFR02I flag
        pl.when(pl.col("tfindr02") == 5).then(1).otherwise(0).alias("tfr02i"),
        
        # PDBIND flag
        pl.when(pl.col("subprod") == "PDB-I").then("Y").otherwise("N").alias("pdbind"),
        
        # SPECIALF handling
        pl.when(pl.col("specialf").is_in(['20', '25', '30'])).then(1).otherwise(0).alias("sfs"),
        pl.when(pl.col("specialf").is_in(['20', '25', '30'])).then(0).otherwise(1).alias("nonsfs"),
        
        # Reset NODAYS if OUTSTAND is 0
        pl.when(
            (pl.col("nodays") > 0) & (pl.col("outstand") < 1)
        ).then(0).otherwise(pl.col("nodays")).alias("nodays"),
        pl.when(
            (pl.col("nodays") > 0) & (pl.col("outstand") < 1)
        ).then(0).otherwise(pl.col("arrears")).alias("arrears"),
        pl.when(
            (pl.col("nodays") > 0) & (pl.col("outstand") < 1)
        ).then(0).otherwise(pl.col("instalm")).alias("instalm"),
        
        # Handle PRODGRP = 'BA'
        pl.when(pl.col("prodgrp") == 'BA').then(pl.col("balance")).otherwise(None).alias("prinamt_myrx_ba"),
        pl.when(pl.col("prodgrp") == 'BA').then(pl.col("unearned")).otherwise(None).alias("intamt_myrx_ba")
    ])
    
    # Summarize BTR2
    btr3a = btr2.group_by(["acctnox", "facility", "forcurr", "pdbind"]).agg([
        pl.col("outstand").sum().alias("outstand"),
        pl.col("instalm").sum().alias("instalm"),
        pl.col("unearned").sum().alias("unearned"),
        pl.col("repaid").sum().alias("repaid"),
        pl.col("disburse").sum().alias("disburse"),
        pl.col("tfr02i").sum().alias("tfr02i"),
        pl.col("mtd_tawidh_amt").sum().alias("mtd_tawidh_amt"),
        pl.col("mtd_gharamah_amt").sum().alias("mtd_gharamah_amt"),
        pl.col("prinamt_myrx").sum().alias("prinamt_myrx"),
        pl.col("intamt_myrx").sum().alias("intamt_myrx"),
        pl.col("oth_chargex").sum().alias("oth_chargex"),
        pl.col("nodays").max().alias("nodays")
    ])
    
    # Get max NODAYS per account
    btr2x = btr2.sort(
        ["acctnox", "facility", "forcurr", "pdbind", "nodays"],
        descending=[False, False, False, False, True]
    ).unique(subset=["acctnox", "facility", "forcurr", "pdbind"], keep="first")
    
    print(f"BTR2 processed: {btr2.height} rows")

# =========================================================
# 12. PROCESS SUBCR
# =========================================================
print("\nProcessing SUBCR...")

subcr = None
if btr2x is not None and btr3a is not None:
    subcr = btr2x.join(btr3a, on=["acctnox", "facility", "forcurr", "pdbind"], how="inner")
    
    subcr = subcr.with_columns([
        (pl.col("outstand").cast(pl.Float64, strict=False) * 100).cast(pl.Int64).alias("outstand"),
        (pl.col("unearned").cast(pl.Float64, strict=False) * 100).cast(pl.Int64).alias("unearned"),
        (pl.col("repaid").cast(pl.Float64, strict=False) * 100).cast(pl.Int64).alias("repaid"),
        (pl.col("disburse").cast(pl.Float64, strict=False) * 100).cast(pl.Int64).alias("disburse"),
        (pl.col("prinamt_myrx").cast(pl.Float64, strict=False) * 100).cast(pl.Int64).alias("curbal"),
        (pl.col("intamt_myrx").cast(pl.Float64, strict=False) * 100).cast(pl.Int64).alias("intamt"),
        (pl.col("oth_chargex").cast(pl.Float64, strict=False) * 100).cast(pl.Int64).alias("oth_charge"),
        pl.lit("    ").alias("noteno"),
        pl.when(pl.col("instalm").is_null()).then(0).otherwise(pl.col("instalm")).alias("instalm")
    ])
    
    # Handle special facilities
    subcr = subcr.with_columns([
        pl.when(
            pl.col("facility").is_in(["34810", "34831", "34832", "34840", "34850", "34860"])
        ).then(0).otherwise(pl.col("arrears")).alias("arrears"),
        pl.when(
            pl.col("facility").is_in(["34810", "34831", "34832", "34840", "34850", "34860"])
        ).then(0).otherwise(pl.col("instalm")).alias("instalm")
    ])
    
    print(f"SUBCR processed: {subcr.height} rows")

# =========================================================
# 13. CREATE FINAL SUBA
# =========================================================
print("\nCreating final SUBA...")

suba_final = None
if mast is not None and subcr is not None:
    suba_final = mast.join(subcr, on="acctnox", how="inner")
    
    # Join with ACCT for APPRLIM2
    if acct is not None:
        acct_subset = acct.select(["acctnox", "apprlim2", "firstdisbdt"]).unique()
        suba_final = suba_final.join(acct_subset, on="acctnox", how="left")
    
    suba_final = suba_final.with_columns([
        pl.lit(" 00000000 00000000").alias("dataxx"),
        pl.lit(0).cast(pl.Int64).alias("odxsamt"),
        pl.lit(0).cast(pl.Int64).alias("biltot"),
        pl.when(pl.col("apprlim2").is_null()).then(0).otherwise(pl.col("apprlim2")).alias("apprlim2"),
        pl.lit(12).cast(pl.Int64).alias("noteterm"),
        pl.lit("N").alias("syndicat"),
        pl.lit("00").alias("specialf"),
        pl.lit("5300").alias("purposes"),
        pl.lit("19").alias("payfreqc"),
        pl.when(pl.col("firstdisbdt") > 0)
          .then(pl.col("firstdisbdt").dt.strftime("%d%m%Y"))
          .otherwise("00000000").alias("fdisbdt"),
        pl.lit("N").alias("sm_status1"),
        pl.lit("00000000").alias("sm_dat1"),
        pl.lit("000000000000000").alias("rmsbba"),
        pl.lit("     ").alias("score1"),
        pl.lit("     ").alias("score2"),
        pl.lit("N").alias("dnbfisme"),
        pl.lit("").alias("lu_add1"),
        pl.lit("").alias("lu_add2"),
        pl.lit("").alias("lu_add3"),
        pl.lit("").alias("lu_add4"),
        pl.lit("").alias("lu_town_city"),
        pl.lit("").alias("lu_postcode"),
        pl.lit("").alias("lu_state_cd"),
        pl.lit("").alias("lu_country_cd"),
        pl.lit("").alias("ia_lru"),
        pl.lit("").alias("sm_status"),
        pl.lit("").alias("sm_datestr")
    ])
    
    # Calculate UNDRAWN
    subq = suba_final.group_by("acctnox").agg([
        pl.col("outstand").sum().alias("outx")
    ])
    suba_final = suba_final.join(subq, on="acctnox", how="left")
    suba_final = suba_final.with_columns([
        (pl.col("apprlim2").cast(pl.Float64, strict=False) - pl.col("outx").cast(pl.Float64, strict=False)).cast(pl.Int64).alias("undrawn")
    ])
    
    print(f"Final SUBA processed: {suba_final.height} rows")

# =========================================================
# 14. WRITE OUTPUT FILES
# =========================================================
print("\nWriting output files...")

output_suffix = f"{REPTYEAR}{REPTMON}{REPTDAY}"

# ACCTCRED
if acct is not None:
    acctcred_output = acct.select([
        pl.col("ficody").alias("FICODY"),
        pl.col("ficode").cast(pl.Int64).alias("FICODE"),
        pl.col("apcode").cast(pl.Int64).alias("APCODE"),
        pl.col("acctnox").cast(pl.Int64).alias("ACCTNO"),
        pl.lit("MYR").alias("CURRENCY"),
        pl.lit(0).cast(pl.Int64).alias("APPRLIMT"),
        pl.lit(0).cast(pl.Int64).alias("APPRLIM2"),
        pl.col("issuedd").cast(pl.Int64).alias("ISSUEDD"),
        pl.col("issuemm").cast(pl.Int64).alias("ISSUEMM"),
        pl.col("issueya").cast(pl.Int64).alias("ISSUEYA"),
        pl.col("issueyy").cast(pl.Int64).alias("ISSUEYY"),
        pl.col("oldbrh").cast(pl.Int64).alias("OLDBRH"),
        pl.col("lmtamt").cast(pl.Int64).alias("LMTAMT"),
        pl.lit(0).cast(pl.Int64).alias("AALIMIT"),
        pl.col("allrefno").fill_null("").alias("ALLREFNO"),
        pl.lit(0).cast(pl.Int64).alias("LEGAL_ACTION_CD"),
        pl.col("ladtdd").cast(pl.Int64).alias("LADTDD"),
        pl.col("ladtmm").cast(pl.Int64).alias("LADTMM"),
        pl.col("ladtyy").cast(pl.Int64).alias("LADTYY"),
        pl.col("fxrate").cast(pl.Int64).alias("FXRATE"),
        pl.col("climate_prin_taxonomy_class").alias("CLIMATE_PRIN_TAXONOMY_CLASS")
    ])
    
    acctcred_spec = [
        ("FICODY", 5, 'S'), ("FICODE", 4, 'Z'), ("APCODE", 3, 'Z'),
        ("ACCTNO", 10, 'Z'), ("CURRENCY", 3, 'S'), ("APPRLIMT", 24, 'Z'),
        ("APPRLIM2", 16, 'Z'), ("ISSUEDD", 2, 'Z'), ("ISSUEMM", 2, 'Z'),
        ("ISSUEYA", 2, 'Z'), ("ISSUEYY", 2, 'Z'), ("OLDBRH", 5, 'Z'),
        ("LMTAMT", 16, 'Z'), ("AALIMIT", 24, 'Z'), ("ALLREFNO", 200, 'S'),
        ("LEGAL_ACTION_CD", 2, 'Z'), ("LADTDD", 2, 'Z'), ("LADTMM", 2, 'Z'),
        ("LADTYY", 4, 'Z'), ("FXRATE", 8, 'Z'), ("CLIMATE_PRIN_TAXONOMY_CLASS", 5, 'S')
    ]
    
    write_fixed_width(acctcred_output, BASE_OUTPUT / f"ACCTCRED_{output_suffix}.txt", acctcred_spec)
    print(f"ACCTCRED written: {acctcred_output.height} records")

# SUBACRED
if suba_final is not None:
    subacred_output = suba_final.select([
        pl.col("ficody").alias("FICODY"),
        pl.col("ficode").cast(pl.Int64).alias("FICODE"),
        pl.col("apcode").cast(pl.Int64).alias("APCODE"),
        pl.col("acctnox").cast(pl.Int64).alias("ACCTNO"),
        pl.col("noteno").alias("NOTENO"),
        pl.col("facility").fill_null("").alias("FACILITY"),
        pl.col("facility").fill_null("").alias("FACILITY2"),
        pl.col("syndicat").alias("SYNDICAT"),
        pl.col("specialf").alias("SPECIALF"),
        pl.col("purposes").alias("PURPOSES"),
        pl.col("fconcept").cast(pl.Int64).alias("FCONCEPT"),
        pl.col("noteterm").cast(pl.Int64).alias("NOTETERM"),
        pl.col("payfreqc").alias("PAYFREQC"),
        pl.col("dataxx").alias("DATAXX"),
        pl.col("custcode_clean").cast(pl.Int64).alias("CUSTCODE"),
        pl.col("sector_clean").alias("SECTOR"),
        pl.col("oldbrh").cast(pl.Int64).alias("OLDBRH"),
        pl.col("unearned").cast(pl.Int64).alias("UNEARNED"),
        pl.col("sm_status1").alias("SM_STATUS1"),
        pl.col("sm_dat1").alias("SM_DAT1"),
        pl.col("rmsbba").alias("RMSBBA"),
        pl.lit(0).cast(pl.Int64).alias("INTRATEX"),
        pl.col("typeprc").alias("TYPEPRC"),
        pl.col("faccode").fill_null("").alias("FACCODE"),
        pl.col("sectfiss").alias("SECTFISS"),
        pl.col("custfiss").alias("CUSTFISS"),
        pl.col("forcurr").fill_null("MYR").alias("FORCURR"),
        pl.col("tfr02i").cast(pl.Int64).alias("TFR02I"),
        pl.lit(0).cast(pl.Int64).alias("COMMRATEX"),
        pl.lit(0).cast(pl.Int64).alias("DISCRATEX"),
        pl.lit(0).cast(pl.Int64).alias("COMBRATEX"),
        pl.col("sm_status").alias("SM_STATUS"),
        pl.col("sm_datestr").alias("SM_DATESTR"),
        pl.col("ia_lru").alias("IA_LRU"),
        pl.col("pdbind").alias("PDBIND"),
        pl.col("fdisbdt").alias("FDISBDT"),
        pl.col("score1").alias("SCORE1"),
        pl.col("score2").alias("SCORE2"),
        pl.col("dnbfisme").alias("DNBFISME"),
        pl.lit("").alias("INDUSTRIAL_SECTOR_CD"),
        pl.col("lu_add1").alias("LU_ADD1"),
        pl.col("lu_add2").alias("LU_ADD2"),
        pl.col("lu_add3").alias("LU_ADD3"),
        pl.col("lu_add4").alias("LU_ADD4"),
        pl.col("lu_town_city").alias("LU_TOWN_CITY"),
        pl.col("lu_postcode").alias("LU_POSTCODE"),
        pl.col("lu_state_cd").alias("LU_STATE_CD"),
        pl.col("lu_country_cd").alias("LU_COUNTRY_CD")
    ])
    
    subacred_spec = [
        ("FICODY", 5, 'S'), ("FICODE", 4, 'Z'), ("APCODE", 3, 'Z'),
        ("ACCTNO", 10, 'Z'), ("NOTENO", 5, 'S'), ("FACILITY", 5, 'S'),
        ("FACILITY2", 5, 'S'), ("SYNDICAT", 1, 'S'), ("SPECIALF", 2, 'S'),
        ("PURPOSES", 4, 'S'), ("FCONCEPT", 2, 'Z'), ("NOTETERM", 3, 'Z'),
        ("PAYFREQC", 2, 'S'), ("DATAXX", 18, 'S'), ("CUSTCODE", 2, 'Z'),
        ("SECTOR", 4, 'S'), ("OLDBRH", 5, 'Z'), ("UNEARNED", 17, 'Z'),
        ("SM_STATUS1", 1, 'S'), ("SM_DAT1", 8, 'S'), ("RMSBBA", 15, 'S'),
        ("INTRATEX", 5, 'Z'), ("TYPEPRC", 2, 'S'), ("FACCODE", 5, 'Z'),
        ("SECTFISS", 4, 'S'), ("CUSTFISS", 2, 'S'), ("FORCURR", 3, 'S'),
        ("TFR02I", 1, 'Z'), ("COMMRATEX", 5, 'Z'), ("DISCRATEX", 5, 'Z'),
        ("COMBRATEX", 5, 'Z'), ("SM_STATUS", 1, 'S'), ("SM_DATESTR", 8, 'S'),
        ("IA_LRU", 1, 'S'), ("PDBIND", 1, 'S'), ("FDISBDT", 8, 'S'),
        ("SCORE1", 5, 'S'), ("SCORE2", 5, 'S'), ("DNBFISME", 1, 'S'),
        ("INDUSTRIAL_SECTOR_CD", 5, 'S'), ("LU_ADD1", 40, 'S'),
        ("LU_ADD2", 40, 'S'), ("LU_ADD3", 40, 'S'), ("LU_ADD4", 40, 'S'),
        ("LU_TOWN_CITY", 20, 'S'), ("LU_POSTCODE", 5, 'S'),
        ("LU_STATE_CD", 2, 'S'), ("LU_COUNTRY_CD", 2, 'S')
    ]
    
    write_fixed_width(subacred_output, BASE_OUTPUT / f"SUBACRED_{output_suffix}.txt", subacred_spec)
    print(f"SUBACRED written: {subacred_output.height} records")

# CREDITPO
if suba_final is not None:
    creditpo_output = suba_final.select([
        pl.col("ficody").alias("FICODY"),
        pl.col("ficode").cast(pl.Int64).alias("FICODE"),
        pl.col("apcode").cast(pl.Int64).alias("APCODE"),
        pl.col("acctnox").cast(pl.Int64).alias("ACCTNO"),
        pl.col("noteno").alias("NOTENO"),
        pl.col("facility").fill_null("").alias("FACILITY"),
        pl.lit(REPTDAY).alias("REPTDAY"),
        pl.lit(REPTMON).alias("REPTMON"),
        pl.lit(REPTYEAR).alias("REPTYEAR"),
        pl.col("outstand").cast(pl.Int64).alias("OUTSTAND"),
        pl.col("arrears").cast(pl.Int64).alias("ARREARS"),
        pl.col("instalm").cast(pl.Int64).alias("INSTALM"),
        pl.col("undrawn").cast(pl.Int64).alias("UNDRAWN"),
        pl.lit("O").alias("ACCTSTAT"),
        pl.col("nodays").cast(pl.Int64).alias("NODAYS"),
        pl.col("oldbrh").cast(pl.Int64).alias("OLDBRH"),
        pl.col("biltot").cast(pl.Int64).alias("BILTOT"),
        pl.col("odxsamt").cast(pl.Int64).alias("ODXSAMT"),
        pl.col("curbal").cast(pl.Int64).alias("CURBAL"),
        pl.col("intamt").cast(pl.Int64).alias("INTAMT"),
        pl.col("oth_charge").cast(pl.Int64).alias("OTH_CHARGE"),
        pl.col("repaid").cast(pl.Int64).alias("REPAID"),
        pl.col("disburse").cast(pl.Int64).alias("DISBURSE"),
        pl.col("faccode").fill_null("").alias("FACCODE"),
        pl.col("forcurr").fill_null("MYR").alias("FORCURR"),
        pl.col("pdbind").alias("PDBIND"),
        pl.col("mtd_tawidh_amt").cast(pl.Int64).alias("MTD_TAWIDH_AMT"),
        pl.col("mtd_gharamah_amt").cast(pl.Int64).alias("MTD_GHARAMAH_AMT"),
        pl.lit("").alias("REPAY_SOURCE"),
        pl.lit("").alias("REPAY_TYPE_CD")
    ])
    
    creditpo_spec = [
        ("FICODY", 5, 'S'), ("FICODE", 4, 'Z'), ("APCODE", 3, 'Z'),
        ("ACCTNO", 10, 'Z'), ("NOTENO", 5, 'S'), ("FACILITY", 5, 'S'),
        ("REPTDAY", 2, 'S'), ("REPTMON", 2, 'S'), ("REPTYEAR", 4, 'S'),
        ("OUTSTAND", 16, 'Z'), ("ARREARS", 3, 'Z'), ("INSTALM", 3, 'Z'),
        ("UNDRAWN", 17, 'Z'), ("ACCTSTAT", 1, 'S'), ("NODAYS", 5, 'Z'),
        ("OLDBRH", 5, 'Z'), ("BILTOT", 17, 'Z'), ("ODXSAMT", 17, 'Z'),
        ("CURBAL", 17, 'Z'), ("INTAMT", 17, 'Z'), ("OTH_CHARGE", 17, 'Z'),
        ("REPAID", 15, 'Z'), ("DISBURSE", 15, 'Z'), ("FACCODE", 5, 'Z'),
        ("FORCURR", 3, 'S'), ("PDBIND", 1, 'S'),
        ("MTD_TAWIDH_AMT", 15, 'D'), ("MTD_GHARAMAH_AMT", 15, 'D'),
        ("REPAY_SOURCE", 4, 'S'), ("REPAY_TYPE_CD", 2, 'S')
    ]
    
    write_fixed_width(creditpo_output, BASE_OUTPUT / f"CREDITPO_{output_suffix}.txt", creditpo_spec)
    print(f"CREDITPO written: {creditpo_output.height} records")

# =========================================================
# 15. PRINT SUMMARY
# =========================================================
print("\n" + "="*50)
print("PROCESSING COMPLETE")
print("="*50)
print(f"Processing Date: {TDATE.strftime('%Y-%m-%d')}")
print(f"MAST rows: {mast.height if mast is not None else 0}")
print(f"CRED rows: {cred.height if cred is not None else 0}")
print(f"SUBA rows: {suba.height if suba is not None else 0}")
print(f"ACCT rows: {acct.height if acct is not None else 0}")
print(f"BTR2 rows: {btr2.height if btr2 is not None else 0}")
print(f"SUBCR rows: {subcr.height if subcr is not None else 0}")
print(f"Final SUBA rows: {suba_final.height if suba_final is not None else 0}")
print(f"\nOutput files written to: {BASE_OUTPUT}")
print("="*50)
