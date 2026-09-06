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

# Read the files
try:
    coll = read_fixed_width_file(COLL_FILE, coll_specs, encoding='cp037')
    desc = read_fixed_width_file(DESC_FILE, desc_specs, encoding='cp037')
    
    # Print sample data for debugging
    print("\n=== COLL Data Sample (first 5 rows) ===")
    print(coll.head(5))
    print(f"\nCOLL columns: {coll.columns}")
    print(f"COLL dtypes: {coll.dtypes}")
    
    print("\n=== DESC Data Sample (first 5 rows) ===")
    print(desc.head(5))
    print(f"\nDESC columns: {desc.columns}")
    print(f"DESC dtypes: {desc.dtypes}")
    
    # Check unique values in DESC for CINSTCL and NATGUAR
    if 'cinstcl' in desc.columns:
        print(f"\nUnique CINSTCL values: {desc['cinstcl'].unique().to_list()[:20]}")
    if 'natguar' in desc.columns:
        print(f"\nUnique NATGUAR values: {desc['natguar'].unique().to_list()[:20]}")
    
    # Ensure ccollno has the same data type in both DataFrames
    coll = coll.with_columns(pl.col("ccollno").cast(pl.Utf8).alias("ccollno"))
    desc = desc.with_columns(pl.col("ccollno").cast(pl.Utf8).alias("ccollno"))
    
    # Convert acctno and noteno to float64 to match LOAN dataframe
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

print(f"\n  COLL rows: {coll.height}")
print(f"  DESC rows: {desc.height}")

# PROC SORT; BY CCOLLNO; (for both COLL and DESC)
coll = coll.sort(by="ccollno")
desc = desc.sort(by="ccollno")

# DATA COLL; MERGE COLL(IN=A) DESC(IN=B); BY CCOLLNO; IF A AND B;
coll = coll.join(desc, on="ccollno", how="inner")
print(f"  COLL rows after join with DESC: {coll.height}")

# Print sample after join
if coll.height > 0:
    print("\n=== COLL after join (first 5 rows) ===")
    print(coll.head(5))

# IF CINSTCL='18' AND NATGUAR='06';
# Try different variations of the filter to see if data exists
if coll.height > 0:
    # Check what values actually exist
    if 'cinstcl' in coll.columns:
        unique_cinstcl = coll['cinstcl'].unique().to_list()
        print(f"\nUnique CINSTCL values after join: {unique_cinstcl[:20]}")
    if 'natguar' in coll.columns:
        unique_natguar = coll['natguar'].unique().to_list()
        print(f"Unique NATGUAR values after join: {unique_natguar[:20]}")
    
    # Try different filter conditions
    filter_18 = coll.filter(pl.col("cinstcl") == "18")
    print(f"\nRows with CINSTCL='18': {filter_18.height}")
    
    filter_06 = coll.filter(pl.col("natguar") == "06")
    print(f"Rows with NATGUAR='06': {filter_06.height}")
    
    filter_both = coll.filter((pl.col("cinstcl") == "18") & (pl.col("natguar") == "06"))
    print(f"Rows with CINSTCL='18' AND NATGUAR='06': {filter_both.height}")

# Apply the actual filter
coll = coll.filter((pl.col("cinstcl") == "18") & (pl.col("natguar") == "06"))

# PROC SORT; BY ACCTNO NOTENO;
coll = coll.sort(by=["acctno", "noteno"])

print(f"\n  COLL rows after filter: {coll.height}")
