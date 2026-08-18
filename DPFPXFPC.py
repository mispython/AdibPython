# =========================================================
# 15. PROCESS REPAID7B
# =========================================================
print("\nProcessing REPAID7B...")

btrpay = None
if btr2 is not None:
    # Debug: Check what columns are available in btr2
    print(f"BTR2 columns available: {[c for c in btr2.columns if 'repaid' in c.lower() or 'repay' in c.lower()]}")
    
    # Debug: Check repaid column values
    if 'repaid' in btr2.columns:
        repaid_stats = btr2.select([
            pl.col("repaid").cast(pl.Float64, strict=False).sum().alias("total_repaid"),
            pl.col("repaid").cast(pl.Float64, strict=False).max().alias("max_repaid"),
            pl.col("repaid").cast(pl.Float64, strict=False).min().alias("min_repaid"),
            (pl.col("repaid").cast(pl.Float64, strict=False) > 0).sum().alias("count_repaid_gt_0")
        ])
        print(f"REPAID stats: {repaid_stats.to_dicts()}")
    
    # Debug: Check repay_source and repay_type_cd columns
    if 'repay_source' in btr2.columns:
        print(f"REPAY_SOURCE values: {btr2['repay_source'].unique().to_list()[:10]}")
    else:
        print("REPAY_SOURCE column not found in BTR2")
    
    if 'repay_type_cd' in btr2.columns:
        print(f"REPAY_TYPE_CD values: {btr2['repay_type_cd'].unique().to_list()[:10]}")
    else:
        print("REPAY_TYPE_CD column not found in BTR2")
    
    # Filter for repaid > 0
    btrpay = btr2.filter(
        pl.col("repaid").cast(pl.Float64, strict=False) > 0
    )
    
    print(f"BTR2 records with repaid > 0: {btrpay.height}")
    
    if not btrpay.is_empty():
        # Sort by account and facility
        btrpay = btrpay.sort(["acctnox", "facility", "forcurr", "pdbind", "repay_source", "repay_type_cd"])
        
        # Debug: Show sample of records
        print("Sample REPAID7B records:")
        print(btrpay.select(["acctnox", "facility", "repaid", "repay_source", "repay_type_cd"]).head(5))
        
        # Aggregate by repayment source and type
        btrpay = btrpay.group_by([
            "acctnox", "facility", "forcurr", "pdbind", "repay_source", "repay_type_cd", "faccode", "ficode"
        ]).agg([
            pl.col("repaid").sum().alias("repaid_amt")
        ])
        
        print(f"REPAID7B aggregated: {btrpay.height} rows")
    else:
        print("No records with repaid > 0 found in BTR2")

# =========================================================
# 16. WRITE OUTPUT FILES
# =========================================================
print("\nWriting output files...")

output_suffix = f"{REPTYEAR}{REPTMON}{REPTDAY}"

# ... (existing output code for ACCTCRED, SUBACRED, CREDITPO)

# REPAID7B
if btrpay is not None and not btrpay.is_empty():
    # Ensure all required columns exist
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
        ("FICODE", 4, 'Z'), ("ACCTNO", 11, 'Z'), ("REPTDAY", 2, 'S'),
        ("REPTMON", 2, 'S'), ("REPTYEAR", 4, 'S'), ("REPAY_SOURCE", 4, 'S'),
        ("REPAY_TYPE_CD", 2, 'S'), ("REPAID_AMT", 16, 'D'),
        ("FACILITY", 5, 'S'), ("FORCURR", 3, 'S'), ("PDBIND", 1, 'S'),
        ("FACCODE", 5, 'S'), ("REPAID", 16, 'D')
    ]
    
    write_fixed_width(repaid7b_output, BASE_OUTPUT / f"REPAID7B_{output_suffix}.txt", repaid7b_spec)
    print(f"REPAID7B written: {repaid7b_output.height} records")
else:
    print(f"REPAID7B: No data to write (0 records with repaid > 0)")
    # Create empty file with headers for completeness
    with open(BASE_OUTPUT / f"REPAID7B_{output_suffix}.txt", 'w') as f:
        f.write("")  # Empty file
    print(f"REPAID7B: Empty file created")
