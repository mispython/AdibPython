from pathlib import Path
import duckdb
import polars as pl
from datetime import datetime, timedelta

# Define paths - UPDATE THESE
SAVING_PATH = Path("/path/to/your/SAVING.csv")
CIS_PATH = Path("/path/to/your/CIS_CUSTDLY.parquet") 
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

# =========================
# STEP 1: REPTDATE - Exact SAS logic
# =========================
# SAS: REPTDATE=INPUT(SUBSTR(PUT(EXTDATE, Z11.), 1, 8), MMDDYY8.)
# Using today()-1 like your original request
REPTDATE = datetime.today() - timedelta(days=1)

# SAS: CALL SYMPUT('REPTYEAR',PUT(REPTDATE,YEAR2.))
reptyear = REPTDATE.strftime("%y")  # 2-digit year

# SAS: CALL SYMPUT('REPTMON',PUT(MM,Z2.))  
reptmon = REPTDATE.strftime("%m")   # 2-digit month with leading zero

# SAS: CALL SYMPUT('REPTDAY', PUT(DAY(REPTDATE), Z2.))
reptday = REPTDATE.strftime("%d")   # 2-digit day with leading zero

# SAS: CALL SYMPUT('REPTDTE', PUT(REPTDATE,Z5.))
# Z5. means 5 digits with leading zeros - this is YYMMD format in SAS
reptdte = int(f"{reptyear}{reptmon}{reptday}")  # Convert to int for comparison

# =========================
# STEP 2: BRIGHT - Exact SAS logic
# =========================
# Load SAVING data
saving = pl.read_csv(
    SAVING_PATH,
    encoding="utf8-lossy",
    infer_schema_length=0,
    ignore_errors=True
)

# SAS: IF PRODUCT=208;
# SAS: IF OPENDT GT 0 THEN DO; OPENDD = SUBSTR(PUT(OPENDT,Z11.),3,2); etc.
# SAS: OPENDT=MDY(OPENMM,OPENDD,OPENYY);
# SAS: IF OPENDT = &REPTDTE;
bright = (
    saving
    .filter(pl.col("PRODUCT") == "208")
    .with_columns(pl.col("OPENDT").cast(pl.Int64))
    .filter(pl.col("OPENDT") > 0)
    .filter(pl.col("OPENDT") == reptdte)  # Exact SAS comparison
    .select(["BRANCH", "ACCTNO", "OPENDT", "CURBAL", "OPENIND"])
)

# SAS: PROC SORT DATA=BRIGHT; BY ACCTNO;
bright = bright.sort("ACCTNO")

# =========================
# STEP 3: CIS - Exact SAS logic  
# =========================
cis = pl.read_parquet(CIS_PATH)

# SAS: IF CUSTOPENDATE GT 0 THEN DO; CUSTDD = SUBSTR(...); etc.
# SAS: CUSTOPDT=MDY(CUSTMM,CUSTDD,CUSTYY);
# SAS: IF PRISEC = 901 THEN DO; PRIS2='N'; END; ELSE DO; PRIS2='Y'; END;
cis_processed = (
    cis
    .with_columns(pl.col("CUSTOPENDATE").cast(pl.Int64))
    .filter(pl.col("CUSTOPENDATE") > 0)
    .with_columns(
        pl.when(pl.col("PRISEC") == 901)
        .then(pl.lit("N"))
        .otherwise(pl.lit("Y"))
        .alias("JOINT")  # SAS: RENAME PRIS2=JOINT
    )
    .select(["ACCTNO", "CUSTNAME", "ALIASKEY", "ALIAS", "CUSTOPENDATE", "JOINT"])
)

# SAS: PROC SORT DATA=CIS; BY ACCTNO;
cis_processed = cis_processed.sort("ACCTNO")

# =========================
# STEP 4: NEW - Exact SAS logic
# =========================
# SAS: MERGE BRIGHT(IN=A) CIS(IN=B); BY ACCTNO; IF A AND B;
new = bright.join(cis_processed, on="ACCTNO", how="inner")

# =========================
# STEP 5: CONVERT - Exact SAS logic
# =========================
# SAS: BRANCH2=LEFT(BRANCH); ACCTNO2=LEFT(ACCTNO); etc.
convert = (
    new
    .with_columns([
        pl.col("BRANCH").str.strip_chars().alias("BRANCH2"),
        pl.col("ACCTNO").str.strip_chars().alias("ACCTNO2"), 
        pl.col("OPENDT").cast(pl.Utf8).str.strip_chars().alias("OPENDT2"),
        pl.col("CURBAL").cast(pl.Utf8).str.strip_chars().alias("CURBAL2"),
        pl.col("JOINT").str.strip_chars().alias("JOINT2"),
        pl.col("CUSTOPENDATE").cast(pl.Utf8).str.strip_chars().alias("CUSTOPD2"),
    ])
    .select([
        pl.col("BRANCH2").alias("BRANCH"),
        pl.col("ACCTNO2").alias("ACCTNO"),
        pl.col("ALIAS").alias("NEWIC"),  # SAS: RENAME ALIAS=NEWIC
        pl.col("JOINT2").alias("JOINT"),
        pl.col("CUSTNAME"),
        pl.col("OPENDT2").alias("OPENDT"), 
        pl.col("OPENIND"),
        pl.col("CUSTOPD2").alias("CUSTOPDT"),
        pl.col("CURBAL2").cast(pl.Float64).alias("CURBAL"),  # SAS: $8.2 format
    ])
)

# SAS: PROC SORT DATA=CONVERT; BY ACCTNO;
convert = convert.sort("ACCTNO")

# =========================
# STEP 6: Save output
# =========================
output_file = OUTPUT_DIR / f"BRIGHTSTAR_SAVINGS_{reptyear}{reptmon}{reptday}.parquet"
convert.write_parquet(output_file)

print(f"Processing completed. Output: {output_file}")
print(f"Records processed: {len(convert)}")
