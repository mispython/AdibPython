# =========================================================
# 13. CREATE FINAL SUBA
# =========================================================
print("\nCreating final SUBA...")

suba_final = None
if mast is not None and subcr is not None:
    # Before joining, select only needed columns from mast to avoid duplicates
    mast_for_join = mast.select([
        "acctnox", "ficody", "ficode", "apcode", "branch", "oldbrh",
        "custcode_clean", "custfiss", "sector_clean", "sectfiss"
    ])
    
    # Join mast with subcr
    suba_final = mast_for_join.join(subcr, on="acctnox", how="inner")
    
    # Join with ACCT for APPRLIM2 and FIRSTDISBDT
    if acct is not None:
        acct_subset = acct.select([
            "acctnox", 
            pl.col("apprlim2").fill_null(0).alias("apprlim2"),
            pl.col("firstdisbdt").fill_null(0).alias("firstdisbdt")
        ]).unique(subset=["acctnox"])
        suba_final = suba_final.join(acct_subset, on="acctnox", how="left")
    
    # Add calculated fields
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
