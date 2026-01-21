import polars as pl
from pathlib import Path

def eibm0941():
    base = Path.cwd()
    dep_path = base / "DEP"
    mis_path = base / "MIS"
    miss_path = base / "MISS"
    depo_path = base / "DEPO"
    idepo_path = base / "IDEPO"
    
    # Execute external program
    try:
        from pbblnfmt import process as pbblnfmt_process
        pbblnfmt_process()
    except ImportError:
        print("Note: PGM(PBBLNFMT) not executed")
    
    # REPTDATE processing
    reptdate_df = pl.read_parquet(dep_path / "REPTDATE.parquet")
    reptdate = reptdate_df["REPTDATE"][0]
    
    mm = reptdate.month
    mm1 = mm - 1 if mm > 1 else 12
    
    rdate = reptdate.strftime("%d%m%y")
    ryear = str(reptdate.year)
    rmonth = f"{mm:02d}"
    reptmon1 = f"{mm1:02d}"
    repdt = reptdate.strftime("%y%m%d")
    
    print(f"RDATE: {rdate}, RYEAR: {ryear}, RMONTH: {rmonth}")
    
    # Helper function to read datasets
    def read_dataset(path, file_name):
        try:
            return pl.read_parquet(path / file_name)
        except:
            return pl.DataFrame()
    
    # 1. SACA: SAVG + CURR
    savg_df = read_dataset(mis_path, f"SAVG{rmonth}.parquet")
    curr_df = read_dataset(mis_path, f"CURR{rmonth}.parquet")
    saca_df = pl.concat([savg_df, curr_df])
    if not saca_df.is_empty():
        saca_df = saca_df.with_columns(pl.lit("PBB ").alias("AGLFLD"))
    
    # 2. FD
    fd_df = read_dataset(mis_path, f"FD{rmonth}.parquet")
    if not fd_df.is_empty():
        fd_df = fd_df.with_columns(
            pl.lit("PBB").alias("AGLFLD"),
            pl.col("NOCD").alias("NOACCT")
        )
    
    # 3. SACAI (Islamic)
    savgf_df = read_dataset(miss_path, f"SAVGF{rmonth}.parquet")
    currf_df = read_dataset(miss_path, f"CURRF{rmonth}.parquet")
    sacai_df = pl.concat([savgf_df, currf_df])
    if not sacai_df.is_empty():
        sacai_df = sacai_df.with_columns(pl.lit("PIBB").alias("AGLFLD"))
    
    # 4. FDI (Islamic)
    fdf_df = read_dataset(miss_path, f"FDF{rmonth}.parquet")
    if not fdf_df.is_empty():
        fdf_df = fdf_df.with_columns(
            pl.lit("PIBB").alias("AGLFLD"),
            pl.col("NOCD").alias("NOACCT")
        )
    
    # 5. Combine DEP datasets
    dep_dfs = []
    for df in [saca_df, fd_df, sacai_df, fdf_df]:
        if not df.is_empty():
            dep_dfs.append(df)
    
    if not dep_dfs:
        print("No DEP data found")
        return
    
    dep_combined = pl.concat(dep_dfs)
    
    # Process DEP data
    dep_df = dep_combined.filter(
        (~pl.col("BRANCH").is_in([119, 132])) &
        (pl.col("BRANCH") <= 800)
    )
    
    # Format BRANCX and PRODUCX
    dep_df = dep_df.with_columns(
        pl.col("BRANCH").cast(pl.Utf8).str.zfill(3).alias("BRANCX"),
        pl.when((pl.col("PRODUCT") >= 300) & (pl.col("PRODUCT") <= 400))
        .then(pl.lit("DFIXED"))
        .when((pl.col("PRODUCT") >= 50) & (pl.col("PRODUCT") <= 198))
        .then(pl.lit("DDMAND"))
        .when((pl.col("PRODUCT") >= 200) & (pl.col("PRODUCT") <= 220))
        .then(pl.lit("DSVING"))
        .otherwise(pl.lit(""))
        .alias("PRODUCX")
    )
    
    # Keep only required columns
    dep_df = dep_df.select(["BRANCX", "PRODUCX", "NOACCT", "AGLFLD"])
    
    # Summary by AGLFLD, BRANCX, PRODUCX
    deps_df = dep_df.group_by(["AGLFLD", "BRANCX", "PRODUCX"]).agg(
        pl.sum("NOACCT").alias("NOACCT")
    )
    
    # Add additional fields
    deps_df = deps_df.with_columns(
        pl.lit("   ").alias("LOB"),
        pl.lit("EXT").alias("BGLFLD"),
        pl.lit("C_DEP").alias("CGLFLD"),
        pl.lit("ACTUAL").alias("DGLFLD"),
        pl.lit(f"{ryear}{rmonth}").alias("YM")
    ).filter(pl.col("NOACCT") > 0)
    
    # 6. RNID (Non-Islamic)
    rnid_df = read_dataset(depo_path, f"RNIDM{rmonth}.parquet")
    if not rnid_df.is_empty():
        rnid_df = rnid_df.filter(
            (pl.col("CDSTAT") == "A") &
            (pl.col("NIDSTAT") == "N")
        )
        rnid_df = rnid_df.with_columns(
            pl.lit(1).alias("NOACCT"),
            pl.col("BRANCH").cast(pl.Utf8).str.zfill(3).alias("BRANCX")
        )
        
        # Summary
        rnid1_df = rnid_df.group_by("BRANCX").agg(
            pl.sum("NOACCT").alias("NOACCT")
        )
        
        # Add fields
        rnid1_df = rnid1_df.with_columns(
            pl.lit("PBB ").alias("AGLFLD"),
            pl.lit("EXT").alias("BGLFLD"),
            pl.lit("C_DEP").alias("CGLFLD"),
            pl.lit("ACTUAL").alias("DGLFLD"),
            pl.lit("DPNIDS").alias("PRODUCX"),
            pl.lit(f"{ryear}{rmonth}").alias("YM")
        )
    else:
        rnid1_df = pl.DataFrame()
    
    # 7. IRNID (Islamic)
    irnid_df = read_dataset(idepo_path, f"RNIDM{rmonth}.parquet")
    if not irnid_df.is_empty():
        irnid_df = irnid_df.filter(
            (pl.col("CDSTAT") == "A") &
            (pl.col("NIDSTAT") == "N")
        )
        irnid_df = irnid_df.with_columns(
            pl.lit(1).alias("NOACCT"),
            pl.col("BRANCH").cast(pl.Utf8).str.zfill(3).alias("BRANCX")
        )
        
        # Summary
        irnid1_df = irnid_df.group_by("BRANCX").agg(
            pl.sum("NOACCT").alias("NOACCT")
        )
        
        # Add fields
        irnid1_df = irnid1_df.with_columns(
            pl.lit("PIBB").alias("AGLFLD"),
            pl.lit("EXT").alias("BGLFLD"),
            pl.lit("C_DEP").alias("CGLFLD"),
            pl.lit("ACTUAL").alias("DGLFLD"),
            pl.lit("DPNIDS").alias("PRODUCX"),
            pl.lit(f"{ryear}{rmonth}").alias("YM")
        )
    else:
        irnid1_df = pl.DataFrame()
    
    # 8. Combine all datasets
    all_dfs = []
    for df in [deps_df, rnid1_df, irnid1_df]:
        if not df.is_empty():
            all_dfs.append(df)
    
    if not all_dfs:
        print("No data to process")
        return
    
    all_df = pl.concat(all_dfs)
    all_df = all_df.sort(["AGLFLD", "BRANCX", "PRODUCX"])
    
    # Add LOB column if missing
    if 'LOB' not in all_df.columns:
        all_df = all_df.with_columns(pl.lit("   ").alias("LOB"))
    
    # 9. Generate GLINT file (similar to GLAPP format)
    generate_glint_file(all_df, base / "GLINT.csv")
    
    print(f"Processing complete. Output: GLINT.csv")
    print(f"Total records: {len(all_df)}")
    
    # Show summary
    if 'AGLFLD' in all_df.columns:
        summary = all_df.group_by("AGLFLD").agg(
            pl.sum("NOACCT").alias("TOTAL_ACCOUNTS"),
            pl.count().alias("RECORDS")
        )
        print("\nSummary by AGLFLD:")
        for row in summary.iter_rows(named=True):
            print(f"  {row['AGLFLD']}: {row['RECORDS']} records, {row['TOTAL_ACCOUNTS']} accounts")

def generate_glint_file(df, output_path):
    """Generate GLINT file with fixed positions (similar to SAS PUT)"""
    if df.is_empty():
        return
    
    # Calculate counters
    cnt = len(df)
    numac = df["NOACCT"].sum() if "NOACCT" in df.columns else 0
    
    with open(output_path, 'w') as f:
        # Write D records
        for idx, row in enumerate(df.iter_rows(named=True)):
            aglfld = str(row.get('AGLFLD', '')).ljust(32)
            bglfld = str(row.get('BGLFLD', 'EXT')).ljust(32)
            cglfld = str(row.get('CGLFLD', 'C_DEP')).ljust(32)
            dglfld = str(row.get('DGLFLD', 'ACTUAL')).ljust(32)
            brancx = str(row.get('BRANCX', '')).ljust(32)
            lob = str(row.get('LOB', '   ')).ljust(32)
            producx = str(row.get('PRODUCX', '')).ljust(32)
            ym = str(row.get('YM', '')).rjust(6)
            noacct = f"{row.get('NOACCT', 0):15.0f}"
            
            # Note: Last field is 'Y,' instead of 'N,' (different from GLAPP)
            line = f"D,{aglfld},{bglfld},{cglfld},{dglfld},,{brancx},{lob},{producx},{ym},{noacct},Y,,,\n"
            f.write(line)
        
        # Write T record at EOF
        f.write(f"T,{cnt:10.0f},{numac:15.0f},\n")
    
    print(f"GLINT file created with {cnt} D records and 1 T record")

# Simplified version
def eibm0941_simple():
    """Simpler version focusing on core logic"""
    base = Path.cwd()
    
    # Get date
    reptdate = pl.read_parquet(base / "DEP/REPTDATE.parquet")["REPTDATE"][0]
    ryear = str(reptdate.year)
    rmonth = f"{reptdate.month:02d}"
    
    # Process each dataset type
    datasets = []
    
    # Process DEP accounts
    dep_types = [
        ("MIS/SAVG", "PBB ", None),  # AGLFLD, NOACCT source
        ("MIS/CURR", "PBB ", None),
        ("MIS/FD", "PBB", "NOCD"),
        ("MISS/SAVGF", "PIBB", None),
        ("MISS/CURRF", "PIBB", None),
        ("MISS/FDF", "PIBB", "NOCD"),
    ]
    
    for path_prefix, aglfld, nocd_col in dep_types:
        try:
            df = pl.read_parquet(base / f"{path_prefix}{rmonth}.parquet")
            # Filter branches
            df = df.filter(
                (~pl.col("BRANCH").is_in([119, 132])) &
                (pl.col("BRANCH") <= 800)
            )
            
            # Add AGLFLD
            df = df.with_columns(pl.lit(aglfld).alias("AGLFLD"))
            
            # Set NOACCT
            if nocd_col:
                df = df.with_columns(pl.col(nocd_col).alias("NOACCT"))
            else:
                df = df.with_columns(pl.lit(1).alias("NOACCT"))
            
            # Format BRANCX
            df = df.with_columns(
                pl.col("BRANCH").cast(pl.Utf8).str.zfill(3).alias("BRANCX")
            )
            
            # Determine PRODUCX
            df = df.with_columns(
                pl.when((pl.col("PRODUCT") >= 300) & (pl.col("PRODUCT") <= 400))
                .then("DFIXED")
                .when((pl.col("PRODUCT") >= 50) & (pl.col("PRODUCT") <= 198))
                .then("DDMAND")
                .when((pl.col("PRODUCT") >= 200) & (pl.col("PRODUCT") <= 220))
                .then("DSVING")
                .otherwise("")
                .alias("PRODUCX")
            )
            
            # Keep only needed columns
            df = df.select(["BRANCX", "PRODUCX", "NOACCT", "AGLFLD"])
            datasets.append(df)
        except:
            pass
    
    # Process RNID accounts
    rnid_types = [
        ("DEPO/RNIDM", "PBB "),
        ("IDEPO/RNIDM", "PIBB"),
    ]
    
    for path_prefix, aglfld in rnid_types:
        try:
            df = pl.read_parquet(base / f"{path_prefix}{rmonth}.parquet")
            df = df.filter(
                (pl.col("CDSTAT") == "A") &
                (pl.col("NIDSTAT") == "N")
            )
            df = df.with_columns(
                pl.lit(1).alias("NOACCT"),
                pl.col("BRANCH").cast(pl.Utf8).str.zfill(3).alias("BRANCX"),
                pl.lit(aglfld).alias("AGLFLD"),
                pl.lit("DPNIDS").alias("PRODUCX")
            ).select(["BRANCX", "PRODUCX", "NOACCT", "AGLFLD"])
            datasets.append(df)
        except:
            pass
    
    if not datasets:
        print("No data found")
        return
    
    # Combine and process
    all_df = pl.concat(datasets)
    
    # Group by AGLFLD, BRANCX, PRODUCX
    result_df = all_df.group_by(["AGLFLD", "BRANCX", "PRODUCX"]).agg(
        pl.sum("NOACCT").alias("NOACCT")
    )
    
    # Add additional fields
    result_df = result_df.with_columns(
        pl.lit("   ").alias("LOB"),
        pl.lit("EXT").alias("BGLFLD"),
        pl.lit("C_DEP").alias("CGLFLD"),
        pl.lit("ACTUAL").alias("DGLFLD"),
        pl.lit(f"{ryear}{rmonth}").alias("YM")
    ).filter(pl.col("NOACCT") > 0)
    
    # Sort
    result_df = result_df.sort(["AGLFLD", "BRANCX", "PRODUCX"])
    
    # Generate output
    generate_glint_file(result_df, base / "GLINT.csv")
    
    print(f"EIBM0941 processing complete: {len(result_df)} records")

if __name__ == "__main__":
    eibm0941_simple()
