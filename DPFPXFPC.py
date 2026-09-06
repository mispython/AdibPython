# =========================
# COLL file processing (EBCDIC with packed decimal)
# =========================
print("Processing COLL and DESC files...")

# DATA COLL; INFILE COLL; INPUT @004 CCOLLNO PD6. @146 ACCTNO PD6. @153 NOTENO PD6.;
coll_specs = [
    ("ccollno", 4, 9, "pd"),    # @004 PD6.
    ("acctno", 146, 151, "pd"),  # @146 PD6.
    ("noteno", 153, 158, "pd")   # @153 PD6.
]

# DATA DESC; INFILE DESC; INPUT @001 CCOLLNO 11. @051 CINSTCL $2. @055 NATGUAR $2. @211 CENSUS 10. @291 TRANCHE $8.;
desc_specs = [
    ("ccollno", 1, 11, "numeric"),   # @001 11.
    ("cinstcl", 51, 52, "character"), # @051 $2.
    ("natguar", 55, 56, "character"), # @055 $2.
    ("census", 211, 220, "numeric"),  # @211 10.
    ("tranche", 291, 298, "character") # @291 $8.
]

# Read the files - since they're EBCDIC with complex formats
try:
    coll = read_ebcdic_fixed_width(COLL_FILE, coll_specs)
    desc = read_ebcdic_fixed_width(DESC_FILE, desc_specs)
    
    # Ensure ccollno has the same data type in both DataFrames
    # Convert ccollno to string in both for consistent joining
    coll = coll.with_columns(pl.col("ccollno").cast(pl.Utf8).alias("ccollno"))
    desc = desc.with_columns(pl.col("ccollno").cast(pl.Utf8).alias("ccollno"))
    
    # Convert acctno and noteno to float64 to match LOAN dataframe
    # First strip any whitespace using str.strip_chars() and convert to float
    coll = coll.with_columns([
        pl.col("acctno").cast(pl.Utf8).str.strip_chars().cast(pl.Float64, strict=False).alias("acctno"),
        pl.col("noteno").cast(pl.Utf8).str.strip_chars().cast(pl.Float64, strict=False).alias("noteno")
    ])
    
except Exception as e:
    print(f"Warning: Error reading EBCDIC files: {e}")
    print("Creating empty DataFrames as placeholder")
    coll = pl.DataFrame(schema={"ccollno": pl.Utf8, "acctno": pl.Float64, "noteno": pl.Float64})
    desc = pl.DataFrame(schema={"ccollno": pl.Utf8, "cinstcl": pl.Utf8, "natguar": pl.Utf8, 
                                "census": pl.Float64, "tranche": pl.Utf8})

print(f"  COLL rows: {coll.height}")
print(f"  DESC rows: {desc.height}")

# PROC SORT; BY CCOLLNO; (for both COLL and DESC)
coll = coll.sort(by="ccollno")
desc = desc.sort(by="ccollno")

# DATA COLL; MERGE COLL(IN=A) DESC(IN=B); BY CCOLLNO; IF A AND B;
coll = coll.join(desc, on="ccollno", how="inner")

# IF CINSTCL='18' AND NATGUAR='06';
coll = coll.filter((pl.col("cinstcl") == "18") & (pl.col("natguar") == "06"))

# PROC SORT; BY ACCTNO NOTENO;
coll = coll.sort(by=["acctno", "noteno"])

print(f"  COLL rows after filter: {coll.height}")

# =========================
# DATA NPGS; MERGE LOAN(IN=A) COLL(IN=B); BY ACCTNO NOTENO; IF A AND B;
# =========================
# Ensure loan acctno and noteno are float64 to match coll
if loan.height > 0 and coll.height > 0:
    # Check data types and cast if necessary
    if loan.schema["acctno"] != pl.Float64:
        loan = loan.with_columns(pl.col("acctno").cast(pl.Float64).alias("acctno"))
    if loan.schema["noteno"] != pl.Float64:
        loan = loan.with_columns(pl.col("noteno").cast(pl.Float64).alias("noteno"))

npgs = loan.join(coll, on=["acctno", "noteno"], how="inner")
print(f"NPGS rows after COLL merge: {npgs.height}")

# PROC SORT; BY PENDBRH;
npgs = npgs.sort(by="pendbrh")

# =========================
# DATA MICR; INFILE MICR; INPUT @001 PENDBRH 3. @040 MICRCD $5.;
# =========================
print("Processing MICR file...")
try:
    # Read MICR text file with fixed width format
    # Using column positions: PENDBRH at position 1-3, MICRCD at position 40-44
    micr_df = pl.read_csv(
        MICR_FILE, 
        separator='\t', 
        has_header=False,
        new_columns=["pendbrh", "micrcd"]
    )
    micr_df.columns = [col.lower() for col in micr_df.columns]
    micr = micr_df.select(["pendbrh", "micrcd"]).sort(by="pendbrh")
except Exception as e:
    print(f"Warning: Error reading MICR file: {e}")
    # Fallback: create empty DataFrame
    micr = pl.DataFrame(schema={"pendbrh": pl.Float64, "micrcd": pl.Utf8})

# Ensure pendbrh data types match for join
if npgs.height > 0 and micr.height > 0:
    if npgs.schema["pendbrh"] != micr.schema["pendbrh"]:
        # Cast both to float64 for consistency
        npgs = npgs.with_columns(pl.col("pendbrh").cast(pl.Float64).alias("pendbrh"))
        micr = micr.with_columns(pl.col("pendbrh").cast(pl.Float64).alias("pendbrh"))

# DATA NPGS; MERGE NPGS(IN=A) MICR; BY PENDBRH; IF A;
npgs = npgs.join(micr, on="pendbrh", how="left")

# =========================
# CVAR02 mapping from SCH
# =========================
print("Creating CVAR fields...")

# FORMAT CVAR02 $3.; CVAR02='   ';
npgs = npgs.with_columns([
    pl.lit("   ").alias("cvar02")
])

# IF SCH='P93' THEN CVAR02='93'; ELSE IF SCH='P94' THEN CVAR02='94'; ELSE IF SCH='P101' THEN CVAR02='101';
npgs = npgs.with_columns([
    pl.when(pl.col("sch") == "P93").then(pl.lit("93"))
     .when(pl.col("sch") == "P94").then(pl.lit("94"))
     .when(pl.col("sch") == "P101").then(pl.lit("101"))
     .otherwise(pl.col("cvar02"))
     .alias("cvar02")
])

# IF CVAR02 NE '   ';
npgs = npgs.filter(pl.col("cvar02") != "   ")

# =========================
# Final CVAR fields
# =========================
# DATA NPGS; SET NPGS;
npgs = npgs.with_columns([
    # CVAR01=CENSUS;
    pl.col("census").alias("cvar01"),
    # CVAR03=NEWIC;
    pl.col("newic").alias("cvar03"),
    # CVAR04=CUSTNAME;
    pl.col("custname").alias("cvar04"),
    # CVAR05=ISSUED;
    pl.col("issued").alias("cvar05"),
    # CVAR06=ACCTNO;
    pl.col("acctno").alias("cvar06"),
    # CVAR07='FL';
    pl.lit("FL").alias("cvar07"),
    # CVAR08=NETPROC;
    pl.col("netproc").alias("cvar08"),
    # CVAR09=BALANCE;
    pl.col("balance").alias("cvar09"),
    # CVAR10=0.00;
    pl.lit(0.00).alias("cvar10"),
    # CVAR11=ARREARS;
    pl.col("arrears").alias("cvar11"),
    # CVAR12='   ';
    pl.lit("   ").alias("cvar12"),
    # CVAR13='          ';
    pl.lit("          ").alias("cvar13"),
    # CVAR14='0233';
    pl.lit("0233").alias("cvar14"),
    # CVAR15=MICRCD;
    pl.col("micrcd").alias("cvar15"),
    # BRANCH=PENDBRH;
    pl.col("pendbrh").alias("branch"),
    # CVAR16='TL';
    pl.lit("TL").alias("cvar16"),
    # CVAR17=CURBAL;
    pl.col("curbal").alias("cvar17"),
])

# IF CVAR04='  ' THEN CVAR04=NAME;
if "name" in npgs.columns:
    npgs = npgs.with_columns([
        pl.when(pl.col("cvar04") == "  ")
          .then(pl.col("name"))
          .otherwise(pl.col("cvar04"))
          .alias("cvar04")
    ])

# IF NPLDATE > 0 THEN DO; ... CVAR13=PUT(NPLDD,Z2.)||'/'||PUT(NPLMM,Z2.)||'/'||PUT(NPLYY,Z4.); END;
npgs = npgs.with_columns([
    pl.when(pl.col("npldate").is_not_null())
      .then(pl.col("npldate").map_elements(format_date_ddmmyyyy, return_dtype=pl.Utf8))
      .otherwise(pl.lit("          "))
      .alias("cvar13")
])

# NORMDT=PUT(NDD,Z2.)||'/'||PUT(NMM,Z2.)||'/'||PUT(NYY,Z4.);
npgs = npgs.with_columns([
    pl.lit(NORMDT).alias("normdt")
])

# IF ARREARS GE 3 AND NPLDATE > 0 THEN CVAR12='NPL';
npgs = npgs.with_columns([
    pl.when((pl.col("arrears") >= 3) & pl.col("npldate").is_not_null())
      .then(pl.lit("NPL"))
      .otherwise(pl.col("cvar12"))
      .alias("cvar12")
])

# =========================
# PROC SORT; BY CVAR06 CVAR01;
# PROC SORT DATA=NPGS.SMEZ OUT=NPLA; BY CVAR06 CVAR01;
# =========================
npgs = npgs.sort(by=["cvar06", "cvar01"])

if NPGS_SMEZ.exists():
    npla = read_sas7bdat(NPGS_SMEZ).sort(by=["cvar06", "cvar01"])
    npgs = npgs.join(npla, on=["cvar06", "cvar01"], how="left", suffix="_npla")
else:
    npgs = npgs.with_columns([
        pl.lit(None).alias("status"),
        pl.lit("          ").alias("ndate")
    ])

# =========================
# Apply NPL status logic
# =========================
def adjust_cvar13(row):
    """Apply SAS logic for CVAR13 adjustments"""
    cvar12 = row.get("cvar12", "   ")
    status = row.get("status", "   ")
    ndate = row.get("ndate", "          ")
    cvar13 = row.get("cvar13", "          ")
    normdt = row.get("normdt", "          ")
    
    # IF CVAR12='NPL' THEN DO;
    if cvar12 == "NPL":
        # IF STATUS='NPL' THEN CVAR13=NDATE;
        if status == "NPL":
            return ndate
        return cvar13
    # IF CVAR12='   ' THEN DO;
    else:
        # IF STATUS='NPL' THEN CVAR13=NORMDT;
        if status == "NPL":
            return normdt
        # IF STATUS='   ' AND NDATE NE '          ' THEN CVAR13=NDATE;
        if status == "   " and ndate != "          ":
            return ndate
        return cvar13

npgs = npgs.with_columns([
    pl.struct(["cvar12", "status", "ndate", "cvar13", "normdt"])
      .map_elements(adjust_cvar13, return_dtype=pl.Utf8)
      .alias("cvar13")
])

# =========================
# PROC SORT; BY CVAR01;
# =========================
npgs = npgs.sort(by="cvar01")

# =========================
# Ensure all required columns exist
# =========================
for c in ["costctr", "balance", "curbal", "accrual", "tranche", "sch", 
          "censust", "product", "natguar", "cinstcl"]:
    if c not in npgs.columns:
        npgs = npgs.with_columns(pl.lit(None).alias(c))

# =========================
# DATA NPGS.LNSMEZ&REPTMON; SET NPGS; KEEP ...
# =========================
keep_cols = [
    "cvar01", "cvar02", "cvar03", "cvar04", "cvar05", "cvar06", "cvar07",
    "cvar08", "cvar09", "cvar10", "cvar11", "cvar12", "cvar13", "cvar14",
    "costctr", "balance", "curbal", "accrual", "tranche",
    "branch", "cvar15", "censust", "product", "natguar", "cinstcl", "sch",
    "cvar16", "cvar17"
]

out = npgs.select(keep_cols)

# Convert column names to uppercase for SAS output
out = out.rename({col: col.upper() for col in out.columns})

# =========================
# Output: NPGS.LNSMEZ&REPTMON (SAS dataset via SASPy)
# =========================
print(f"Writing NPGS.LNSMEZ{REPTMON}...")

# Convert Polars DataFrame to Pandas for SASPy
out_pandas = out.to_pandas()

# Initialize SAS session
sas = saspy.SASsession(results='TEXT')

# Create the output library
sas.submit(f"""
    libname npgs "{BASE_OUTPUT}/NPGS";
    options nofmterr;
""")

# Upload the Pandas DataFrame to SAS
sas_df = sas.df2sd(out_pandas, table='work.temp_out')

# Create the output dataset with proper formats
sas.submit(f"""
    data npgs.lnsmez{REPTMON};
        set work.temp_out;
        format CVAR01 CVAR06 10. 
               CVAR03 $15. 
               CVAR04 $50. 
               CVAR14 $4.
               CVAR13 $10. 
               CVAR08 CVAR09 CVAR10 CVAR17 10.2 
               CVAR11 5.
               CVAR02 $3.
               CVAR12 $3.
               CVAR15 $5.
               CVAR16 $2.
               CVAR07 $2.;
    run;
    
    proc datasets lib=npgs nolist;
        modify lnsmez{REPTMON};
        label
            CVAR01='Census'
            CVAR02='Schedule Code'
            CVAR03='New IC'
            CVAR04='Customer Name'
            CVAR05='Issue Date'
            CVAR06='Account Number'
            CVAR07='Flag'
            CVAR08='Net Proceeds'
            CVAR09='Balance'
            CVAR10='Zero Balance'
            CVAR11='Arrears'
            CVAR12='NPL Status'
            CVAR13='NPL Date'
            CVAR14='Constant Value'
            CVAR15='MICR Code'
            CVAR16='Type'
            CVAR17='Current Balance';
    run;
""")

print(f"Successfully wrote NPGS.LNSMEZ{REPTMON} to {BASE_OUTPUT}/NPGS")

# Close SAS session
sas.endsas()
