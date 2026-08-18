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

def write_fixed_width_positioned(df, filepath, columns_spec):
    """Write DataFrame as fixed-width text file with SAS positioning"""
    if df is None or df.is_empty():
        print(f"Warning: No data to write for {filepath}")
        return
    
    with open(filepath, 'w') as f:
        for row in df.iter_rows(named=True):
            max_pos = max(spec[1] + spec[2] for spec in columns_spec)
            line = [' '] * (max_pos + 10)
            
            for col_name, start_pos, width, format_type in columns_spec:
                value = row.get(col_name, "")
                
                if format_type == 'Z':  # Zero-padded integer (SAS Z format)
                    if value is None or value == '' or pd.isna(value):
                        formatted = '0' * width
                    else:
                        try:
                            formatted = f"{int(float(value)):0{width}d}"
                        except:
                            formatted = '0' * width
                elif format_type == 'I':  # Right-justified integer (SAS numeric format)
                    if value is None or value == '' or pd.isna(value):
                        formatted = ' ' * width
                    else:
                        try:
                            formatted = f"{int(float(value)):{width}d}"
                        except:
                            formatted = ' ' * width
                elif format_type == 'S':  # Left-justified string (SAS $ format)
                    if value is None or pd.isna(value):
                        formatted = ''
                    else:
                        formatted = str(value)[:width].ljust(width)
                elif format_type == 'D':  # Decimal (SAS numeric with decimal)
                    if value is None or value == '' or pd.isna(value):
                        formatted = ' ' * width
                    else:
                        try:
                            formatted = f"{float(value):{width}.2f}"
                        except:
                            formatted = ' ' * width
                else:
                    if value is None or pd.isna(value):
                        formatted = ''
                    else:
                        formatted = str(value)[:width].ljust(width)
                
                for i, char in enumerate(formatted):
                    pos = start_pos + i
                    if pos < len(line):
                        line[pos] = char
            
            f.write(''.join(line).rstrip() + "\n")

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
# 5. PROCESS MAST
# =========================================================
print("\nProcessing MAST...")

mast = None
if data.get('imast') is not None:
    mast = data['imast'].filter(
        pl.col("acctnox").cast(pl.Float64, strict=False) > 2500000000
    )
    
    mast = mast.with_columns([
        pl.col("ficode").alias("branch"),
        pl.lit(0).cast(pl.Int64).alias("oldbrh"),
        pl.lit("     ").alias("ficody")
    ])
    
    mast = mast.with_columns([
        pl.when(pl.col("retailid") == 'C')
          .then(999)
          .otherwise(0)
          .cast(pl.Int64)
          .alias("apcode")
    ])
    
    mast = mast.with_columns([
        pl.when(
            (pl.col("custcodx").is_null()) | (pl.col("custcodx") == "") | (pl.col("custcodx") == "nan")
        ).then(pl.lit("99")).otherwise(pl.col("custcodx")).alias("custcode_clean")
    ])
    
    mast = mast.with_columns([
        pl.col("custcode_clean").cast(pl.Float64, strict=False).map_elements(
            lambda x: put(x, "LOCUSTCD", "99"), return_dtype=pl.Utf8
        ).alias("custfiss")
    ])
    
    mast = mast.with_columns([
        pl.when(
            (pl.col("sector").is_null()) | (pl.col("sector") == "") | (pl.col("sector") == "nan")
        ).then(pl.lit("9999")).otherwise(pl.col("sector")).alias("sector_clean")
    ])
    
    mast = mast.with_columns([
        pl.col("sector_clean").map_elements(
            lambda x: put(x, "RVRSE", "9999"), return_dtype=pl.Utf8
        ).alias("sectfiss")
    ])
    
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
    
    mast = mast.with_columns([
        pl.when(
            pl.col("custfiss").is_in(['77', '78', '95', '96'])
        ).then(pl.lit("9700")).otherwise(pl.col("sectfiss")).alias("sectfiss")
    ])
    
    mast = mast.with_columns([
        pl.col("acctnox").cast(pl.Float64, strict=False)
          .cast(pl.Int64, strict=False)
          .cast(pl.Utf8)
          .alias("acctnox")
    ])
    
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
            mast2_agg = mast2_filtered.with_columns([
                pl.col("acctnox").cast(pl.Float64, strict=False)
                  .cast(pl.Int64, strict=False)
                  .cast(pl.Utf8)
                  .alias("acctnox")
            ]).group_by("acctnox").agg([
                pl.col("aano").cast(pl.Utf8).str.join("|").alias("allrefno"),
                pl.col("apvdate").filter(pl.col("apvdate") > 0).min().alias("firstdisbdt")
            ])
    
    if 'facno' in mast2_df.columns and 'ccpt_ltst_review_dt' in mast2_df.columns:
        mast2c = mast2_df.select([
            pl.col("acctnox").cast(pl.Float64, strict=False)
              .cast(pl.Int64, strict=False)
              .cast(pl.Utf8)
              .alias("acctnox"),
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
    
    cred = cred.with_columns([
        pl.when(
            (pl.col("maturedx").cast(pl.Utf8) == "000000") | 
            (pl.col("maturedx").cast(pl.Utf8).str.strip_chars() == "")
        ).then(pl.lit(99999)).otherwise(
            pl.col("maturedx").cast(pl.Utf8).str.strptime(pl.Date, "%y%m%d").cast(pl.Int64)
        ).alias("matureds")
    ])
    
    cred = cred.with_columns([
        pl.when(
            (pl.col("matureds") > 0) & (pl.col("matureds") <= RDATE)
        ).then(
            pl.lit(RDATE) - pl.col("matureds") + 1
        ).otherwise(pl.lit(0)).alias("nodays")
    ])
    
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
    
    cred = cred.with_columns([
        pl.col("acctnox").cast(pl.Float64, strict=False)
          .cast(pl.Int64, strict=False)
          .cast(pl.Utf8)
          .alias("acctnox")
    ])
    
    cred = cred.unique(subset=["acctnox", "transref"])
    print(f"CRED processed: {cred.height} rows")

# =========================================================
# 8. PROCESS BNM TRADE DATA
# =========================================================
print("\nProcessing BNM Trade data...")

if data.get('ibtrad') is not None and cred is not None:
    transref_col = 'transrex' if 'transrex' in data['ibtrad'].columns else 'transref'
    acctno_col = 'acctnox' if 'acctnox' in data['ibtrad'].columns else 'acctno'
    
    btrax = data['ibtrad'].select([
        pl.col(acctno_col).cast(pl.Float64, strict=False)
          .cast(pl.Int64, strict=False)
          .cast(pl.Utf8)
          .alias("acctnox"),
        pl.col(transref_col).cast(pl.Utf8).str.slice(0, 7).alias("transrex"),
        pl.col("repaid").cast(pl.Float64, strict=False),
        pl.col("disburse").cast(pl.Float64, strict=False),
        pl.col("mtd_tawidh_amt").cast(pl.Float64, strict=False),
        pl.col("mtd_gharamah_amt").cast(pl.Float64, strict=False)
    ])
    
    btrad = data['ibtrad'].filter(
        pl.col("balance").cast(pl.Float64, strict=False) > 0
    ).select([
        pl.col(acctno_col).cast(pl.Float64, strict=False)
          .cast(pl.Int64, strict=False)
          .cast(pl.Utf8)
          .alias("acctnox"),
        pl.col(transref_col).cast(pl.Utf8).str.slice(0, 7).alias("transrex"),
        pl.col("balance").cast(pl.Float64, strict=False),
        pl.col("intrecv").cast(pl.Float64, strict=False),
        pl.col("unearned").cast(pl.Float64, strict=False),
        pl.col("liabcode"),
        pl.col("utrdf")
    ])
    
    if data.get('ibtdtl') is not None:
        ibtdtl_transref_col = 'transrex' if 'transrex' in data['ibtdtl'].columns else 'transref'
        ibtdtl_acctno_col = 'acctnox' if 'acctnox' in data['ibtdtl'].columns else 'acctno'
        
        intrt = data['ibtdtl'].select([
            pl.col(ibtdtl_acctno_col).cast(pl.Float64, strict=False)
              .cast(pl.Int64, strict=False)
              .cast(pl.Utf8)
              .alias("acctnox"),
            pl.col(ibtdtl_transref_col).cast(pl.Utf8).str.slice(0, 7).alias("transrex"),
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
    
    cred = cred.join(btrad, on=["acctnox", "transrex"], how="left", suffix="_btrad")
    cred = cred.join(btrax, on=["acctnox", "transrex"], how="left", suffix="_btrax")
    
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
# 9. PROCESS SUBA
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
    
    suba = suba.with_columns([
        pl.col("acctnox").cast(pl.Float64, strict=False)
          .cast(pl.Int64, strict=False)
          .cast(pl.Utf8)
          .alias("acctnox")
    ])
    
    suba = suba.with_columns([
        pl.when(
            ~pl.col("liabcode").cast(pl.Utf8).is_in(["FFS", "FFU", "FCS", "FCU", "FFL", "FTI", "FTL"])
        ).then(pl.lit("MYR")).otherwise(pl.lit(None)).alias("forcurr"),
        
        pl.col("tfdesc01").cast(pl.Utf8).str.slice(0, 13).alias("aano"),
        
        pl.col("liabcode").cast(pl.Utf8).map_elements(
            lambda x: liab_format(x), return_dtype=pl.Utf8
        ).alias("facility"),
        
        pl.col("liabcode").cast(pl.Utf8).map_elements(
            lambda x: nsrsliab_format(x), return_dtype=pl.Utf8
        ).alias("faccode"),
        
        pl.col("liabcode").cast(pl.Utf8).map_elements(
            lambda x: prctype_format(x), return_dtype=pl.Utf8
        ).alias("typeprc"),
        
        pl.col("liabcode").cast(pl.Utf8).map_elements(
            lambda x: prctypesfs_format(x), return_dtype=pl.Utf8
        ).alias("typeprc_sfs"),
        
        pl.col("liabcode").cast(pl.Utf8).map_elements(
            lambda x: btfcept_format(x), return_dtype=pl.Utf8
        ).alias("fconcept")
    ])
    
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
    if mast2c is not None:
        suba9 = suba9.join(mast2c, on="acctnox", how="left")
    
    acct = mast.join(suba9, on="acctnox", how="inner")
    
    if mast2_agg is not None:
        acct = acct.join(mast2_agg, on="acctnox", how="left")
    
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
        pl.lit("     ").alias("climate_prin_taxonomy_class"),
        pl.lit(0).cast(pl.Int64).alias("legal_action_cd")
    ])
    
    acct = acct.unique(subset=["acctnox"], keep="first")
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
        pl.when(
            (pl.col("utrdf").cast(pl.Utf8) == 'R') & 
            (pl.col("liabcode").cast(pl.Utf8).is_in(['BAE', 'BEI']))
        ).then(pl.lit("34471"))
        .when(
            (pl.col("utrdf").cast(pl.Utf8) == 'R') & 
            (pl.col("liabcode").cast(pl.Utf8).is_in(['BAI', 'BII']))
        ).then(pl.lit("34472"))
        .when(
            (pl.col("utrdf").cast(pl.Utf8) == 'R') & 
            (pl.col("liabcode").cast(pl.Utf8).is_in(['BAP', 'BAS', 'BPI', 'BSI']))
        ).then(pl.lit("34475"))
        .otherwise(pl.col("facility"))
        .alias("facility"),
        
        pl.when(pl.col("tfindr02").cast(pl.Utf8) == "5").then(1).otherwise(0).alias("tfr02i"),
        
        pl.when(pl.col("subprod").cast(pl.Utf8) == "PDB-I")
          .then(pl.lit("Y"))
          .otherwise(pl.lit("N"))
          .alias("pdbind"),
        
        pl.when(pl.col("specialf").cast(pl.Utf8).is_in(['20', '25', '30']))
          .then(1)
          .otherwise(0)
          .alias("sfs"),
        pl.when(pl.col("specialf").cast(pl.Utf8).is_in(['20', '25', '30']))
          .then(0)
          .otherwise(1)
          .alias("nonsfs"),
        
        pl.when(
            (pl.col("nodays") > 0) & (pl.col("outstand").cast(pl.Float64, strict=False) < 1)
        ).then(0).otherwise(pl.col("nodays")).alias("nodays"),
        pl.when(
            (pl.col("nodays") > 0) & (pl.col("outstand").cast(pl.Float64, strict=False) < 1)
        ).then(0).otherwise(pl.col("arrears")).alias("arrears"),
        pl.when(
            (pl.col("nodays") > 0) & (pl.col("outstand").cast(pl.Float64, strict=False) < 1)
        ).then(0).otherwise(pl.col("instalm")).alias("instalm")
    ])
    
    prodgrp_col = 'prodgrp' if 'prodgrp' in btr2.columns else 'prodgrp_right'
    if prodgrp_col in btr2.columns:
        btr2 = btr2.with_columns([
            pl.when(pl.col(prodgrp_col).cast(pl.Utf8) == 'BA')
              .then(pl.col("balance"))
              .otherwise(pl.lit(None))
              .alias("prinamt_myrx_ba"),
            pl.when(pl.col(prodgrp_col).cast(pl.Utf8) == 'BA')
              .then(pl.col("unearned"))
              .otherwise(pl.lit(None))
              .alias("intamt_myrx_ba")
        ])
    
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
    btr3a_renamed = btr3a.rename({
        "outstand": "outstand_sum",
        "instalm": "instalm_sum",
        "unearned": "unearned_sum",
        "repaid": "repaid_sum",
        "disburse": "disburse_sum",
        "tfr02i": "tfr02i_sum",
        "mtd_tawidh_amt": "mtd_tawidh_amt_sum",
        "mtd_gharamah_amt": "mtd_gharamah_amt_sum",
        "prinamt_myrx": "prinamt_myrx_sum",
        "intamt_myrx": "intamt_myrx_sum",
        "oth_chargex": "oth_chargex_sum",
        "nodays": "nodays_max"
    })
    
    subcr = btr2x.join(btr3a_renamed, on=["acctnox", "facility", "forcurr", "pdbind"], how="inner")
    
    subcr = subcr.with_columns([
        (pl.col("outstand_sum").cast(pl.Float64, strict=False) * 100).cast(pl.Int64).alias("outstand"),
        (pl.col("unearned_sum").cast(pl.Float64, strict=False) * 100).cast(pl.Int64).alias("unearned"),
        (pl.col("repaid_sum").cast(pl.Float64, strict=False) * 100).cast(pl.Int64).alias("repaid"),
        (pl.col("disburse_sum").cast(pl.Float64, strict=False) * 100).cast(pl.Int64).alias("disburse"),
        (pl.col("prinamt_myrx_sum").cast(pl.Float64, strict=False) * 100).cast(pl.Int64).alias("curbal"),
        (pl.col("intamt_myrx_sum").cast(pl.Float64, strict=False) * 100).cast(pl.Int64).alias("intamt"),
        (pl.col("oth_chargex_sum").cast(pl.Float64, strict=False) * 100).cast(pl.Int64).alias("oth_charge"),
        pl.lit("    ").alias("noteno"),
        pl.when(pl.col("instalm_sum").is_null()).then(0).otherwise(pl.col("instalm_sum")).alias("instalm"),
        pl.col("nodays_max").alias("nodays"),
        pl.col("tfr02i_sum").alias("tfr02i"),
        pl.col("mtd_tawidh_amt_sum").alias("mtd_tawidh_amt"),
        pl.col("mtd_gharamah_amt_sum").alias("mtd_gharamah_amt")
    ])
    
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
    subcr_for_join = subcr.select([
        "acctnox", "facility", "faccode", "forcurr", "pdbind",
        "outstand", "unearned", "repaid", "disburse", "curbal", 
        "intamt", "oth_charge", "noteno", "instalm", "nodays",
        "arrears", "tfr02i", "mtd_tawidh_amt", "mtd_gharamah_amt",
        "typeprc", "fconcept"
    ])
    
    mast_for_join = mast.select([
        "acctnox", "ficody", "ficode", "apcode", "branch", "oldbrh",
        "custcode_clean", "custfiss", "sector_clean", "sectfiss"
    ])
    
    suba_final = mast_for_join.join(subcr_for_join, on="acctnox", how="inner")
    
    if acct is not None:
        acct_subset = acct.select([
            "acctnox",
            pl.when(pl.col("limtcurf").is_not_null())
              .then(pl.col("limtcurf") * 100)
              .otherwise(0)
              .alias("apprlim2"),
            pl.col("firstdisbdt").fill_null(0).alias("firstdisbdt")
        ]).unique(subset=["acctnox"])
        suba_final = suba_final.join(acct_subset, on="acctnox", how="left")
    else:
        suba_final = suba_final.with_columns([
            pl.lit(0).cast(pl.Int64).alias("apprlim2"),
            pl.lit(0).cast(pl.Int64).alias("firstdisbdt")
        ])
    
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
          .then(pl.col("firstdisbdt").cast(pl.Int64).cast(pl.Utf8).str.zfill(8))
          .otherwise(pl.lit("00000000"))
          .alias("fdisbdt"),
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
        pl.lit("").alias("sm_datestr"),
        pl.lit(0).cast(pl.Int64).alias("intratex"),
        pl.lit(0).cast(pl.Int64).alias("commratex"),
        pl.lit(0).cast(pl.Int64).alias("discratex"),
        pl.lit(0).cast(pl.Int64).alias("combratex"),
        pl.lit("").alias("industrial_sector_cd")
    ])
    
    subq = suba_final.group_by("acctnox").agg([
        pl.col("outstand").sum().alias("outx")
    ])
    suba_final = suba_final.join(subq, on="acctnox", how="left")
    suba_final = suba_final.with_columns([
        (pl.col("apprlim2").cast(pl.Float64, strict=False) - pl.col("outx").cast(pl.Float64, strict=False)).cast(pl.Int64).alias("undrawn")
    ])
    
    print(f"Final SUBA processed: {suba_final.height} rows")

# =========================================================
# 14. PROCESS PROVISIONS
# =========================================================
print("\nProcessing PROVISIONS...")

provi = None
if data.get('iprov') is not None and btr2 is not None:
    provi = data['iprov'].join(
        btr2.select(["acctnox", "transrex", "nodays", "outstand", "facility", "arrears"]).unique(),
        on=["acctnox", "transrex"],
        how="inner"
    )
    
    provi = provi.with_columns([
        pl.when(
            (pl.col("nodays") >= 90) & (pl.col("nodays") <= 182)
        ).then(pl.lit("D"))
        .when(pl.col("nodays") > 182).then(pl.lit("B"))
        .when(pl.col("nplind").cast(pl.Utf8) == "P").then(pl.lit("P"))
        .otherwise(pl.lit("P"))
        .alias("classify"),
        
        pl.when(
            (pl.col("nodays") >= 90) | (pl.col("nplind").cast(pl.Utf8) == "F")
        ).then(pl.lit("Y")).otherwise(pl.lit("N")).alias("impaired")
    ])
    
    print(f"PROVISIONS processed: {provi.height} rows")

# =========================================================
# 15. PROCESS REPAID7B
# =========================================================
print("\nProcessing REPAID7B...")

btrpay = None
if btr2 is not None:
    btrpay = btr2.filter(
        pl.col("repaid").cast(pl.Float64, strict=False) > 0
    ).sort(["acctnox", "facility", "forcurr", "pdbind", "repay_source", "repay_type_cd"])
    
    if not btrpay.is_empty():
        btrpay = btrpay.group_by([
            "acctnox", "facility", "forcurr", "pdbind", "repay_source", "repay_type_cd", "faccode", "ficode"
        ]).agg([
            pl.col("repaid").sum().alias("repaid_amt")
        ])
    
    print(f"REPAID7B processed: {btrpay.height if btrpay is not None else 0} rows")

# =========================================================
# 16. WRITE OUTPUT FILES
# =========================================================
print("\nWriting output files...")

output_suffix = f"{REPTYEAR}{REPTMON}{REPTDAY}"

# ACCTCRED - with SAS positions
if acct is not None:
    acctcred_output = acct.select([
        pl.col("ficody").alias("FICODY"),
        pl.col("ficode").cast(pl.Int64, strict=False).fill_null(0).alias("FICODE"),
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
        pl.col("legal_action_cd").cast(pl.Int64).alias("LEGAL_ACTION_CD"),
        pl.col("ladtdd").cast(pl.Int64).alias("LADTDD"),
        pl.col("ladtmm").cast(pl.Int64).alias("LADTMM"),
        pl.col("ladtyy").cast(pl.Int64).alias("LADTYY"),
        pl.col("fxrate").cast(pl.Int64).alias("FXRATE"),
        pl.col("climate_prin_taxonomy_class").fill_null("").alias("CLIMATE_PRIN_TAXONOMY_CLASS")
    ])
    
    acctcred_spec = [
        ("FICODY", 0, 5, 'S'),
        ("FICODE", 5, 4, 'I'),
        ("APCODE", 9, 3, 'Z'),
        ("ACCTNO", 12, 10, 'Z'),
        ("CURRENCY", 42, 3, 'S'),
        ("APPRLIMT", 45, 24, 'Z'),
        ("APPRLIM2", 69, 16, 'Z'),
        ("ISSUEDD", 85, 2, 'Z'),
        ("ISSUEMM", 87, 2, 'Z'),
        ("ISSUEYA", 89, 2, 'Z'),
        ("ISSUEYY", 91, 2, 'Z'),
        ("OLDBRH", 93, 5, 'Z'),
        ("LMTAMT", 98, 16, 'Z'),
        ("AALIMIT", 114, 24, 'Z'),
        ("ALLREFNO", 139, 200, 'S'),
        ("LEGAL_ACTION_CD", 340, 2, 'Z'),
        ("LADTDD", 355, 2, 'Z'),
        ("LADTMM", 357, 2, 'Z'),
        ("LADTYY", 359, 4, 'Z'),
        ("FXRATE", 364, 8, 'Z'),
        ("CLIMATE_PRIN_TAXONOMY_CLASS", 379, 5, 'S')
    ]
    
    write_fixed_width_positioned(acctcred_output, BASE_OUTPUT / f"ACCTCRED_{output_suffix}.txt", acctcred_spec)
    print(f"ACCTCRED written: {acctcred_output.height} records")

# SUBACRED - with SAS positions
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
        pl.col("custcode_clean").cast(pl.Int64, strict=False).fill_null(0).alias("CUSTCODE"),
        pl.col("sector_clean").fill_null("").alias("SECTOR"),
        pl.col("oldbrh").cast(pl.Int64).alias("OLDBRH"),
        pl.col("unearned").cast(pl.Int64).alias("UNEARNED"),
        pl.col("sm_status1").alias("SM_STATUS1"),
        pl.col("sm_dat1").alias("SM_DAT1"),
        pl.col("rmsbba").alias("RMSBBA"),
        pl.col("intratex").cast(pl.Int64).alias("INTRATEX"),
        pl.col("typeprc").fill_null("99").alias("TYPEPRC"),
        pl.col("faccode").fill_null("").alias("FACCODE"),
        pl.col("sectfiss").alias("SECTFISS"),
        pl.col("custfiss").alias("CUSTFISS"),
        pl.col("forcurr").fill_null("MYR").alias("FORCURR"),
        pl.col("tfr02i").cast(pl.Int64).alias("TFR02I"),
        pl.col("commratex").cast(pl.Int64).alias("COMMRATEX"),
        pl.col("discratex").cast(pl.Int64).alias("DISCRATEX"),
        pl.col("combratex").cast(pl.Int64).alias("COMBRATEX"),
        pl.col("sm_status").alias("SM_STATUS"),
        pl.col("sm_datestr").alias("SM_DATESTR"),
        pl.col("ia_lru").alias("IA_LRU"),
        pl.col("pdbind").alias("PDBIND"),
        pl.col("fdisbdt").alias("FDISBDT"),
        pl.col("score1").alias("SCORE1"),
        pl.col("score2").alias("SCORE2"),
        pl.col("dnbfisme").alias("DNBFISME"),
        pl.col("industrial_sector_cd").alias("INDUSTRIAL_SECTOR_CD"),
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
        ("FICODY", 0, 5, 'S'),
        ("FICODE", 5, 4, 'I'),
        ("APCODE", 9, 3, 'Z'),
        ("ACCTNO", 12, 10, 'Z'),
        ("NOTENO", 42, 5, 'S'),
        ("FACILITY", 47, 5, 'S'),
        ("FACILITY2", 72, 5, 'S'),
        ("SYNDICAT", 77, 1, 'S'),
        ("SPECIALF", 78, 2, 'S'),
        ("PURPOSES", 80, 4, 'S'),
        ("FCONCEPT", 84, 2, 'Z'),
        ("NOTETERM", 86, 3, 'Z'),
        ("PAYFREQC", 89, 2, 'S'),
        ("DATAXX", 91, 18, 'S'),
        ("CUSTCODE", 109, 2, 'Z'),
        ("SECTOR", 111, 4, 'S'),
        ("OLDBRH", 115, 5, 'Z'),
        ("UNEARNED", 120, 17, 'Z'),
        ("SM_STATUS1", 137, 1, 'S'),
        ("SM_DAT1", 138, 8, 'S'),
        ("RMSBBA", 182, 15, 'S'),
        ("INTRATEX", 197, 5, 'Z'),
        ("TYPEPRC", 202, 2, 'S'),
        ("FACCODE", 205, 5, 'Z'),
        ("SECTFISS", 211, 4, 'S'),
        ("CUSTFISS", 216, 2, 'S'),
        ("FORCURR", 219, 3, 'S'),
        ("TFR02I", 223, 1, 'Z'),
        ("COMMRATEX", 225, 5, 'Z'),
        ("DISCRATEX", 231, 5, 'Z'),
        ("COMBRATEX", 237, 5, 'Z'),
        ("SM_STATUS", 243, 1, 'S'),
        ("SM_DATESTR", 245, 8, 'S'),
        ("IA_LRU", 254, 1, 'S'),
        ("PDBIND", 256, 1, 'S'),
        ("FDISBDT", 257, 8, 'S'),
        ("SCORE1", 265, 5, 'S'),
        ("SCORE2", 270, 5, 'S'),
        ("DNBFISME", 275, 1, 'S'),
        ("INDUSTRIAL_SECTOR_CD", 276, 5, 'S'),
        ("LU_ADD1", 289, 40, 'S'),
        ("LU_ADD2", 329, 40, 'S'),
        ("LU_ADD3", 369, 40, 'S'),
        ("LU_ADD4", 409, 40, 'S'),
        ("LU_TOWN_CITY", 449, 20, 'S'),
        ("LU_POSTCODE", 469, 5, 'S'),
        ("LU_STATE_CD", 474, 2, 'S'),
        ("LU_COUNTRY_CD", 476, 2, 'S')
    ]
    
    write_fixed_width_positioned(subacred_output, BASE_OUTPUT / f"SUBACRED_{output_suffix}.txt", subacred_spec)
    print(f"SUBACRED written: {subacred_output.height} records")

# CREDITPO - with SAS positions
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
        ("FICODY", 0, 5, 'S'),
        ("FICODE", 5, 4, 'I'),
        ("APCODE", 9, 3, 'Z'),
        ("ACCTNO", 12, 10, 'Z'),
        ("NOTENO", 42, 5, 'S'),
        ("FACILITY", 42, 5, 'S'),
        ("REPTDAY", 72, 2, 'S'),
        ("REPTMON", 74, 2, 'S'),
        ("REPTYEAR", 76, 4, 'S'),
        ("OUTSTAND", 80, 16, 'Z'),
        ("ARREARS", 96, 3, 'Z'),
        ("INSTALM", 99, 3, 'Z'),
        ("UNDRAWN", 102, 17, 'Z'),
        ("ACCTSTAT", 119, 1, 'S'),
        ("NODAYS", 120, 5, 'Z'),
        ("OLDBRH", 125, 5, 'I'),
        ("BILTOT", 130, 17, 'Z'),
        ("ODXSAMT", 147, 17, 'Z'),
        ("CURBAL", 209, 17, 'Z'),
        ("INTAMT", 226, 17, 'Z'),
        ("OTH_CHARGE", 243, 17, 'Z'),
        ("REPAID", 260, 15, 'Z'),
        ("DISBURSE", 275, 15, 'Z'),
        ("FACCODE", 291, 5, 'I'),
        ("FORCURR", 297, 3, 'S'),
        ("PDBIND", 301, 1, 'S'),
        ("MTD_TAWIDH_AMT", 302, 15, 'D'),
        ("MTD_GHARAMAH_AMT", 317, 15, 'D'),
        ("REPAY_SOURCE", 332, 4, 'S'),
        ("REPAY_TYPE_CD", 336, 2, 'S')
    ]
    
    write_fixed_width_positioned(creditpo_output, BASE_OUTPUT / f"CREDITPO_{output_suffix}.txt", creditpo_spec)
    print(f"CREDITPO written: {creditpo_output.height} records")

# PROVISIO - with SAS positions
if provi is not None:
    provi = provi.with_columns([
        pl.lit(0).cast(pl.Int64).alias("apcode"),
        pl.lit(0).cast(pl.Int64).alias("oldbrh"),
        pl.lit("     ").alias("ficody"),
        pl.lit("MYR").alias("forcurr"),
        pl.lit("N").alias("pdbind"),
        pl.lit("").alias("faccode"),
        pl.lit(0).cast(pl.Int64).alias("curbal"),
        pl.lit(0).cast(pl.Int64).alias("tenor_int"),
        pl.lit(0).cast(pl.Int64).alias("oth_charge"),
        pl.lit(0).cast(pl.Int64).alias("iisamt"),
        pl.lit(0).cast(pl.Int64).alias("totiisr"),
        pl.lit(0).cast(pl.Int64).alias("writeoff")
    ])
    
    provisio_output = provi.select([
        pl.col("ficody").alias("FICODY"),
        pl.col("ficode").cast(pl.Int64, strict=False).fill_null(0).alias("FICODE"),
        pl.col("apcode").cast(pl.Int64).alias("APCODE"),
        pl.col("acctnox").cast(pl.Int64, strict=False).fill_null(0).alias("ACCTNO"),
        pl.col("facility").fill_null("").alias("FACILITY"),
        pl.lit(REPTDAY).alias("REPTDAY"),
        pl.lit(REPTMON).alias("REPTMON"),
        pl.lit(REPTYEAR).alias("REPTYEAR"),
        pl.col("classify").fill_null("P").alias("CLASSIFY"),
        pl.col("arrears").cast(pl.Int64, strict=False).fill_null(0).alias("ARREARS"),
        pl.col("curbal").cast(pl.Int64).alias("CURBAL"),
        pl.col("tenor_int").cast(pl.Int64).alias("TENOR_INT"),
        pl.col("oth_charge").cast(pl.Int64).alias("OTH_CHARGE"),
        pl.lit(0).cast(pl.Int64).alias("REALISVL"),
        pl.lit(0).cast(pl.Int64).alias("IISOPBAL"),
        pl.col("iisamt").cast(pl.Int64).alias("TOTIIS"),
        pl.col("totiisr").cast(pl.Int64).alias("TOTIISR"),
        pl.col("writeoff").cast(pl.Int64).alias("TOTWOF"),
        pl.lit(0).cast(pl.Int64).alias("IISDANAH"),
        pl.lit(0).cast(pl.Int64).alias("IISTRANS"),
        pl.lit(0).cast(pl.Int64).alias("SPOPBAL"),
        pl.lit(0).cast(pl.Int64).alias("SPCHARGE"),
        pl.lit(0).cast(pl.Int64).alias("SPWBAMT"),
        pl.lit(0).cast(pl.Int64).alias("SPWOAMT"),
        pl.lit(0).cast(pl.Int64).alias("SPDANAH"),
        pl.lit(0).cast(pl.Int64).alias("SPTRANS"),
        pl.lit(" ").alias("GP3IND"),
        pl.col("oldbrh").cast(pl.Int64).alias("OLDBRH"),
        pl.col("faccode").fill_null("").alias("FACCODE"),
        pl.col("impaired").fill_null("N").alias("IMPAIRED"),
        pl.col("forcurr").fill_null("MYR").alias("FORCURR"),
        pl.lit(0).cast(pl.Int64).alias("TOTILM"),
        pl.col("pdbind").alias("PDBIND")
    ])
    
    provisio_spec = [
        ("FICODY", 0, 5, 'S'),
        ("FICODE", 5, 4, 'I'),
        ("APCODE", 9, 3, 'Z'),
        ("ACCTNO", 12, 10, 'Z'),
        ("FACILITY", 42, 5, 'S'),
        ("REPTDAY", 72, 2, 'S'),
        ("REPTMON", 74, 2, 'S'),
        ("REPTYEAR", 76, 4, 'S'),
        ("CLASSIFY", 80, 1, 'S'),
        ("ARREARS", 81, 3, 'Z'),
        ("CURBAL", 84, 17, 'Z'),
        ("TENOR_INT", 101, 17, 'Z'),
        ("OTH_CHARGE", 118, 16, 'Z'),
        ("REALISVL", 134, 17, 'Z'),
        ("IISOPBAL", 151, 17, 'Z'),
        ("TOTIIS", 168, 17, 'Z'),
        ("TOTIISR", 185, 17, 'Z'),
        ("TOTWOF", 202, 17, 'Z'),
        ("IISDANAH", 219, 17, 'Z'),
        ("IISTRANS", 236, 17, 'Z'),
        ("SPOPBAL", 253, 17, 'Z'),
        ("SPCHARGE", 270, 17, 'Z'),
        ("SPWBAMT", 287, 17, 'Z'),
        ("SPWOAMT", 304, 17, 'Z'),
        ("SPDANAH", 321, 17, 'Z'),
        ("SPTRANS", 338, 17, 'Z'),
        ("GP3IND", 355, 1, 'S'),
        ("OLDBRH", 356, 5, 'I'),
        ("FACCODE", 362, 5, 'I'),
        ("IMPAIRED", 368, 1, 'S'),
        ("FORCURR", 370, 3, 'S'),
        ("TOTILM", 374, 17, 'Z'),
        ("PDBIND", 392, 1, 'S')
    ]
    
    write_fixed_width_positioned(provisio_output, BASE_OUTPUT / f"PROVISIO_{output_suffix}.txt", provisio_spec)
    print(f"PROVISIO written: {provisio_output.height} records")

# REPAID7B - with SAS positions
if btrpay is not None and not btrpay.is_empty():
    btrpay = btrpay.with_columns([
        pl.col("ficode").cast(pl.Int64, strict=False).fill_null(0).alias("ficode"),
        pl.col("repay_source").fill_null("").alias("repay_source"),
        pl.col("repay_type_cd").fill_null("").alias("repay_type_cd"),
        pl.col("facility").fill_null("").alias("facility"),
        pl.col("forcurr").fill_null("MYR").alias("forcurr"),
        pl.col("pdbind").fill_null("N").alias("pdbind"),
        pl.col("faccode").fill_null("").alias("faccode")
    ])
    
    repaid7b_output = btrpay.select([
        pl.col("ficode").cast(pl.Int64).alias("FICODE"),
        pl.col("acctnox").cast(pl.Int64, strict=False).fill_null(0).alias("ACCTNO"),
        pl.lit(REPTDAY).alias("REPTDAY"),
        pl.lit(REPTMON).alias("REPTMON"),
        pl.lit(REPTYEAR).alias("REPTYEAR"),
        pl.col("repay_source").alias("REPAY_SOURCE"),
        pl.col("repay_type_cd").alias("REPAY_TYPE_CD"),
        pl.col("repaid_amt").cast(pl.Float64).alias("REPAID_AMT"),
        pl.col("facility").alias("FACILITY"),
        pl.col("forcurr").alias("FORCURR"),
        pl.col("pdbind").alias("PDBIND"),
        pl.col("faccode").alias("FACCODE"),
        pl.col("repaid_amt").cast(pl.Float64).alias("REPAID")
    ])
    
    repaid7b_spec = [
        ("FICODE", 0, 4, 'I'),
        ("ACCTNO", 5, 11, 'Z'),
        ("REPTDAY", 18, 2, 'S'),
        ("REPTMON", 20, 2, 'S'),
        ("REPTYEAR", 22, 4, 'S'),
        ("REPAY_SOURCE", 27, 4, 'S'),
        ("REPAY_TYPE_CD", 32, 2, 'S'),
        ("REPAID_AMT", 35, 16, 'D'),
        ("FACILITY", 52, 5, 'S'),
        ("FORCURR", 58, 3, 'S'),
        ("PDBIND", 62, 1, 'S'),
        ("FACCODE", 64, 5, 'S'),
        ("REPAID", 70, 16, 'D')
    ]
    
    write_fixed_width_positioned(repaid7b_output, BASE_OUTPUT / f"REPAID7B_{output_suffix}.txt", repaid7b_spec)
    print(f"REPAID7B written: {repaid7b_output.height} records")
else:
    print(f"REPAID7B: No data to write")
    with open(BASE_OUTPUT / f"REPAID7B_{output_suffix}.txt", 'w') as f:
        f.write("")
    print(f"REPAID7B: Empty file created")

# =========================================================
# 17. PRINT SUMMARY
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
print(f"PROVI rows: {provi.height if provi is not None else 0}")
print(f"BTRPAY rows: {btrpay.height if btrpay is not None else 0}")
print(f"\nOutput files written to: {BASE_OUTPUT}")
print("="*50)
