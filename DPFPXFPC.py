# =========================================================
# 11. PROCESS BTR2
# =========================================================
print("\nProcessing BTR2...")

btr2 = None
btr2x = None
btr3a = None

if cred is not None and suba_main is not None:
    btr2 = cred.join(suba_main, on=["acctnox", "transref"], how="inner")
    
    # Fix column names and add calculated fields
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
        
        # TFR02I flag (check if tfindr02 exists)
        pl.when(pl.col("tfindr02") == 5).then(1).otherwise(0).alias("tfr02i"),
        
        # PDBIND flag (check if subprod exists)
        pl.when(pl.col("subprod") == "PDB-I").then(pl.lit("Y")).otherwise(pl.lit("N")).alias("pdbind"),
        
        # SPECIALF handling (use specialf from SUBA)
        pl.when(pl.col("specialf").cast(pl.Utf8).is_in(['20', '25', '30'])).then(1).otherwise(0).alias("sfs"),
        pl.when(pl.col("specialf").cast(pl.Utf8).is_in(['20', '25', '30'])).then(0).otherwise(1).alias("nonsfs"),
        
        # Reset NODAYS if OUTSTAND is 0
        pl.when(
            (pl.col("nodays") > 0) & (pl.col("outstand") < 1)
        ).then(0).otherwise(pl.col("nodays")).alias("nodays"),
        pl.when(
            (pl.col("nodays") > 0) & (pl.col("outstand") < 1)
        ).then(0).otherwise(pl.col("arrears")).alias("arrears"),
        pl.when(
            (pl.col("nodays") > 0) & (pl.col("outstand") < 1)
        ).then(0).otherwise(pl.col("instalm")).alias("instalm")
    ])
    
    # Handle PRODGRP = 'BA' separately (prodgrp might have _right suffix)
    prodgrp_col = 'prodgrp' if 'prodgrp' in btr2.columns else 'prodgrp_right'
    if prodgrp_col in btr2.columns:
        btr2 = btr2.with_columns([
            pl.when(pl.col(prodgrp_col) == 'BA').then(pl.col("balance")).otherwise(None).alias("prinamt_myrx_ba"),
            pl.when(pl.col(prodgrp_col) == 'BA').then(pl.col("unearned")).otherwise(None).alias("intamt_myrx_ba")
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
