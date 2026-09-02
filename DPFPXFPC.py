from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
import polars as pl
import pyreadstat
import saspy
import tempfile
import os


# =========================
# Paths
# =========================
BASE_INPUT  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS")
BASE_OUTPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRCGCS")
BASE_OUTPUT.mkdir(parents=True, exist_ok=True)

# ---- Inputs (SAS7BDAT) mirroring SAS libs/members ----
MNITB_CURRENT  = BASE_INPUT / "intg_dp_acct_current_m{reptmon}.sas7bdat"    # SAS: MNITB.CURRENT
MNILN_LNNOTE   = BASE_INPUT / "enrh_ln_note_m{reptmon}.sas7bdat"     # SAS: MNILN.LNNOTE

CRFTABL        = BASE_INPUT / "crftabl.txt"   # Text file: SAP.PBB.BTRADE.CRFTABL

# ---- Output ----
OUT_DIR  = BASE_OUTPUT / "excp"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = OUT_DIR / "npgsexcp.sas7bdat"


# =========================
# Helper(s)
# =========================
def read_sas7bdat(file_path: Path) -> pl.DataFrame:
    """Read SAS7BDAT file and convert to Polars DataFrame with lowercase columns."""
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    df, meta = pyreadstat.read_sas7bdat(str(file_path))
    # Convert to Polars and lowercase all column names
    pl_df = pl.from_pandas(df)
    pl_df = pl_df.rename({col: col.lower() for col in pl_df.columns})
    return pl_df


def read_text_file(file_path: Path) -> pl.DataFrame:
    """Read text file (space or tab delimited) and convert to Polars DataFrame with lowercase columns."""
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    # Try to read as CSV with whitespace delimiter
    # You may need to adjust delimiter based on your file format
    try:
        # Try space-delimited first
        df = pl.read_csv(file_path, separator=" ", has_header=True)
    except:
        # Fall back to tab-delimited
        df = pl.read_csv(file_path, separator="\t", has_header=True)
    
    # Lowercase all column names
    df = df.rename({col: col.lower() for col in df.columns})
    return df


def read_flat_file_to_temp_parquet(file_path: Path, temp_dir: str) -> Path:
    """Read binary flat file and convert to temporary parquet for processing."""
    # This is a placeholder - you'll need to specify the actual binary format
    # Based on your description, these files might be in a specific binary format
    # You might need to use numpy.fromfile or struct module to read them
    
    # Example using pandas/numpy to read binary data
    import numpy as np
    
    # Create temporary parquet file
    temp_parquet = Path(temp_dir) / f"{file_path.stem}_temp.parquet"
    
    # TODO: Implement actual binary file reading logic here
    # This will depend on the specific binary format of your files
    # You might need something like:
    # data = np.fromfile(file_path, dtype=np.float64)  # or appropriate dtype
    # Then reshape and create DataFrame
    
    # Placeholder - replace with actual implementation
    print(f"Warning: Using placeholder for binary file reading: {file_path}")
    temp_df = pl.DataFrame()  # Empty DataFrame as placeholder
    
    temp_df.write_parquet(temp_parquet)
    return temp_parquet


# =========================
# 1) Calculate REPTDATE as yesterday
# =========================
REPTDATE = datetime.now() - timedelta(days=1)

REPTMON   = f"{REPTDATE.month:02d}"         # PUT(MM, Z2.)
REPTYEAR2 = f"{REPTDATE.year % 100:02d}"    # PUT(REPTDATE, YEAR2.)
REPTDAY   = f"{REPTDATE.day:02d}"           # PUT(DAY(REPTDATE), Z2.)

# Update file paths with date variables
MNITB_CURRENT = Path(str(MNITB_CURRENT).format(reptmon=REPTMON))
MNILN_LNNOTE = Path(str(MNILN_LNNOTE).format(reptmon=REPTMON))

# BTRSA.MAST&REPTDAY&REPTMON dataset
MAST_FILE = BASE_INPUT / f"btmast{REPTMON}{REPTDAY}{REPTYEAR2}.sas7bdat"

# LCCRISEX files (binary flat files)
COLL_FILE = BASE_INPUT / f"lccrisex_{REPTDATE.year}{REPTMON}{REPTDAY}"
DESC_FILE = BASE_INPUT / f"lccrisex.desc_{REPTDATE.year}{REPTMON}{REPTDAY}"


# =========================
# 2) CRFT from CRFTABL.TXT (text file), filter & map SCH, keep SCH=='   '
# =========================
crft = read_text_file(CRFTABL)

# SAS INPUT fields expected: rectyp1, tfid, subacct, preind, censust, acctno
crft = (
    crft.filter(pl.col("rectyp1") != "1")  # IF RECTYP1='1' THEN DELETE
    .select([
        "tfid", "subacct", "preind", "censust", "acctno"
    ])
    .with_columns([
        pl.lit("   ").alias("sch")
    ])
    .with_columns([
        pl.when(pl.col("censust") == 3).then("P51")
         .when(pl.col("censust") == 4).then("P72")
         .when(pl.col("censust") == 5).then("P65")
         .otherwise(pl.col("sch"))
         .alias("sch")
    ])
    # Keep only unmapped ('   ') as in SAS: "IF SCH EQ '   ';"
    .filter(pl.col("sch") == "   ")
)

# NODUPKEY BY acctno censust subacct
crft = crft.unique(subset=["acctno", "censust", "subacct"], keep="first")

# Merge with MAST (BTRSA.MAST&REPTDAY&REPTMON) by acctno; IF A AND B
if not MAST_FILE.exists():
    raise FileNotFoundError(f"Expected MAST file not found: {MAST_FILE}")

mast = read_sas7bdat(MAST_FILE)
mast = mast.select(["acctno"]).unique(subset=["acctno"], keep="first")

crft = crft.join(mast, on="acctno", how="inner")
crft = crft.filter(pl.col("acctno") > 0).with_columns([
    pl.lit(0).alias("noteno"),
    pl.lit(0).alias("product"),
])

# NODUPKEY BY acctno subacct
crft = crft.unique(subset=["acctno", "subacct"], keep="first")

# KEEP acctno censust product noteno
crft = crft.select(["acctno", "censust", "product", "noteno"])


# =========================
# 3) CA from MNITB.CURRENT (SAS7BDAT) (map→SCH; keep SCH=='   ')
# =========================
ca = read_sas7bdat(MNITB_CURRENT)

ca = (
    ca.select(["acctno", "censust", "product"])
    .with_columns([
        pl.lit(0).alias("noteno"),
        pl.lit("   ").alias("sch")
    ])
    .with_columns([
        pl.when((pl.col("product") == 112) & (pl.col("censust") == 301)).then("P70")
         .when((pl.col("product") == 112) & (pl.col("censust") == 300)).then("P51")
         .when((pl.col("product") == 112) & (pl.col("censust") == 302)).then("P72")
         .when((pl.col("product") == 114) & (pl.col("censust") == 303)).then("P72")
         .when((pl.col("product") == 108) & (pl.col("censust") == 304)).then("P75")
         .otherwise(pl.col("sch"))
         .alias("sch")
    ])
    .filter(pl.col("sch") == "   ")  # keep only unmapped
    .select(["acctno", "censust", "product", "noteno"])
)


# =========================
# 4) LN from MNILN.LNNOTE (SAS7BDAT) (map→SCH; keep SCH=='   ')
# =========================
ln = read_sas7bdat(MNILN_LNNOTE)

ln = (
    ln.select(["acctno", "noteno", "loantype", "census"])
    .with_columns([
        pl.col("loantype").alias("product"),
        pl.col("census").alias("censust"),
        pl.lit("   ").alias("sch"),
    ])
    .with_columns([
        pl.when((pl.col("loantype") == 510) & (pl.col("census").is_in([5.12, 5.13]))).then("P70")
         .when((pl.col("loantype") == 532) & (pl.col("census") == 3.00)).then("P51")
         .when((pl.col("loantype") == 524) & (pl.col("census") == 5.16)).then("P72")
         .when((pl.col("loantype") == 527) & (pl.col("census") == 5.17)).then("P72")
         .when((pl.col("loantype") == 531) & (pl.col("census") == 5.00)).then("P63")
         .when((pl.col("loantype") == 533) & (pl.col("census") == 533.01)).then("P64")
         .when((pl.col("loantype") == 533) & (pl.col("census") == 533.00)).then("P65")
         .otherwise(pl.col("sch"))
         .alias("sch")
    ])
    .filter(pl.col("sch") == "   ")  # keep only unmapped
    .select(["acctno", "noteno", "product", "censust"])
)


# =========================
# 5) COLL/DESC merge (binary flat files), filter DESC census range, then BY acctno
# =========================
# Create temporary directory for binary file conversion
with tempfile.TemporaryDirectory() as temp_dir:
    # Convert binary flat files to temporary parquet
    coll_temp = read_flat_file_to_temp_parquet(COLL_FILE, temp_dir)
    desc_temp = read_flat_file_to_temp_parquet(DESC_FILE, temp_dir)
    
    # Read the temporary parquet files
    coll = pl.read_parquet(coll_temp)
    desc = pl.read_parquet(desc_temp)
    
    # Lowercase column names
    coll = coll.rename({col: col.lower() for col in coll.columns})
    desc = desc.rename({col: col.lower() for col in desc.columns})

# Select and filter
coll = coll.select(["ccollno", "acctno"])
desc = desc.select(["ccollno", "cinstcl", "natguar", "census"])

# Filter DESC census range: (51000000 <= census <= 1099999999)
desc = desc.filter((pl.col("census") >= 51000000) & (pl.col("census") <= 1099999999))

# IF A AND B -> inner join on ccollno
coll = coll.join(desc, on="ccollno", how="inner")


# =========================
# 6) AAA = SET CA LN CRFT; sort BY acctno
# =========================
aaa = pl.concat(
    [
        ca.select(["acctno", "censust", "product", "noteno"]),
        ln.select(["acctno", "censust", "product", "noteno"]),
        crft.select(["acctno", "censust", "product", "noteno"]),
    ],
    how="vertical",
    rechunk=True
).sort(by=["acctno"])


# =========================
# 7) EXCP.NPGSEXCP = MERGE AAA(IN=A) COLL(IN=B) BY acctno; IF A AND B
# =========================
excp = aaa.join(coll, on="acctno", how="inner")

# =========================
# 8) Write output using SASpy
# =========================
# Initialize SAS session
sas = saspy.SASsession(cfgname='default')  # You may need to configure this

# Convert Polars DataFrame to pandas for SASpy
excp_pandas = excp.to_pandas()

# Upload the DataFrame to SAS
sas_df = sas.df2sd(excp_pandas, 'work_excp')

# Save as SAS7BDAT
sas_code = f"""
libname outlib "{OUT_DIR}";
data outlib.npgsexcp;
    set work_excp;
run;
"""

sas.submit(sas_code)

print(f"Wrote {OUT_FILE}")

# Close SAS session
sas.endsas()
