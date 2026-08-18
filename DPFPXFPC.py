# =========================================================
# 8. PROCESS BNM TRADE DATA (with debugging)
# =========================================================
print("\nProcessing BNM Trade data...")

if data.get('ibtrad') is not None and cred is not None:
    # Debug: Check IBTrad columns
    print(f"\n--- DEBUG: IBTrad Data ---")
    print(f"IBTrad has 'transrex' column: {'transrex' in data['ibtrad'].columns}")
    print(f"IBTrad has 'transref' column: {'transref' in data['ibtrad'].columns}")
    print(f"IBTrad has 'acctnox' column: {'acctnox' in data['ibtrad'].columns}")
    print(f"IBTrad has 'acctno' column: {'acctno' in data['ibtrad'].columns}")
    
    # Determine correct column names for IBTrad
    transref_col = 'transrex' if 'transrex' in data['ibtrad'].columns else 'transref'
    acctno_col = 'acctnox' if 'acctnox' in data['ibtrad'].columns else 'acctno'
    
    print(f"Using columns: transref={transref_col}, acctno={acctno_col}")
    
    # BTRAX - Repaid/Disburse (use correct column names)
    btrax = data['ibtrad'].select([
        pl.col(acctno_col).cast(pl.Utf8).alias("acctnox"),
        pl.col(transref_col).cast(pl.Utf8).alias("transrex"),
        pl.col("repaid").cast(pl.Float64, strict=False),
        pl.col("disburse").cast(pl.Float64, strict=False),
        pl.col("mtd_tawidh_amt").cast(pl.Float64, strict=False),
        pl.col("mtd_gharamah_amt").cast(pl.Float64, strict=False)
    ])
    
    # Debug: Check BTRAX
    print(f"\nBTRAX records: {btrax.height}")
    print(f"BTRAX acctnox type: {btrax['acctnox'].dtype}")
    print(f"CRED acctnox type: {cred['acctnox'].dtype}")
    print(f"BTRAX transrex type: {btrax['transrex'].dtype}")
    print(f"CRED transrex type: {cred['transrex'].dtype}")
    
    # Show sample of BTRAX
    print(f"BTRAX sample records:")
    print(btrax.head(3))
    print(f"CRED sample records:")
    print(cred.select(['acctnox', 'transrex']).head(3))
    
    # BTRAD - Balance data (use correct column names)
    btrad = data['ibtrad'].filter(
        pl.col("balance").cast(pl.Float64, strict=False) > 0
    ).select([
        pl.col(acctno_col).cast(pl.Utf8).alias("acctnox"),
        pl.col(transref_col).cast(pl.Utf8).alias("transrex"),
        pl.col("balance").cast(pl.Float64, strict=False),
        pl.col("intrecv").cast(pl.Float64, strict=False),
        pl.col("unearned").cast(pl.Float64, strict=False),
        pl.col("liabcode"),
        pl.col("utrdf")
    ])
    
    # INTRT - Interest rates (use correct column names)
    if data.get('ibtdtl') is not None:
        ibtdtl_transref_col = 'transrex' if 'transrex' in data['ibtdtl'].columns else 'transref'
        ibtdtl_acctno_col = 'acctnox' if 'acctnox' in data['ibtdtl'].columns else 'acctno'
        
        intrt = data['ibtdtl'].select([
            pl.col(ibtdtl_acctno_col).cast(pl.Utf8).alias("acctnox"),
            pl.col(ibtdtl_transref_col).cast(pl.Utf8).alias("transrex"),
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
    
    # Debug: Check for matching records
    matching_count = cred.join(btrax.select(["acctnox", "transrex"]).unique(), 
                               on=["acctnox", "transrex"], 
                               how="inner").height
    print(f"\nMatching records between CRED and BTRAX: {matching_count}")
    
    # Merge with CRED
    cred = cred.join(btrad, on=["acctnox", "transrex"], how="left", suffix="_btrad")
    cred = cred.join(btrax, on=["acctnox", "transrex"], how="left", suffix="_btrax")
    
    # Debug: Check repaid after join
    if 'repaid' in cred.columns:
        cred_repaid_stats = cred.select([
            pl.col("repaid").cast(pl.Float64, strict=False).sum().alias("total_repaid"),
            pl.col("repaid").cast(pl.Float64, strict=False).max().alias("max_repaid"),
            (pl.col("repaid").cast(pl.Float64, strict=False) > 0).sum().alias("count_repaid_gt_0")
        ])
        print(f"\nCRED REPAID after join: {cred_repaid_stats.to_dicts()}")
    else:
        print(f"\nWARNING: 'repaid' not in CRED after join")
        print(f"CRED columns with 'repaid': {[c for c in cred.columns if 'repaid' in c.lower()]}")
    
    # Update OUTSTAND and other fields
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
    
    # Debug: Final repaid stats
    cred_final_repaid = cred.select([
        pl.col("repaid").cast(pl.Float64, strict=False).sum().alias("total_repaid"),
        pl.col("repaid").cast(pl.Float64, strict=False).max().alias("max_repaid"),
        (pl.col("repaid").cast(pl.Float64, strict=False) > 0).sum().alias("count_repaid_gt_0")
    ])
    print(f"\nFinal CRED REPAID stats: {cred_final_repaid.to_dicts()}")
    
    print(f"BNM Trade data processed")
