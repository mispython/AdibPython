# =========================================================
# 12. PROCESS SUBCR
# =========================================================
print("\nProcessing SUBCR...")

subcr = None
if btr2x is not None and btr3a is not None:
    # Before joining, rename columns in btr3a to avoid duplicates
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
    
    # Join with renamed columns
    subcr = btr2x.join(btr3a_renamed, on=["acctnox", "facility", "forcurr", "pdbind"], how="inner")
    
    # Apply transformations using the renamed columns
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
