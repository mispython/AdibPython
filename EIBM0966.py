import polars as pl
from pathlib import Path

def eibm0966():
    base = Path.cwd()
    elds_path = base / "ELDS"
    eldsi_path = base / "ELDSI"
    
    # Execute external program
    try:
        from pbblnfmt import process as pbblnfmt_process
        pbblnfmt_process()
    except ImportError:
        print("Note: PGM(PBBLNFMT) not executed")
    
    # REPTDATE processing (same as 0963)
    reptdate_df = pl.read_parquet(elds_path / "REPTDATE.parquet")
    reptdate = reptdate_df["REPTDATE"][0]
    
    wk1, wk2, wk3, wk4 = '1', '2', '3', '4'
    reptmon = f"{reptdate.month:02d}"
    rmonth = f"{reptdate.month:02d}"
    reptyear = str(reptdate.year)[-2:]
    ryear = str(reptdate.year)
    
    # Read LNFMT file
    lnfmt_lines = []
    try:
        with open(base / "LNFMT", 'r') as f:
            for line in f:
                if len(line) >= 44:
                    product = line[5:8].strip()
                    producx = line[26:33].strip()
                    lob = line[39:43].strip()
                    lnfmt_lines.append({"PRODUCT": product, "PRODUCX": producx, "LOB": lob})
    except:
        lnfmt_lines = []
    
    lnfmt_df = pl.DataFrame(lnfmt_lines).unique("PRODUCT")
    
    # Read DRFMT file
    try:
        drfmt_df = pl.read_csv(base / "DRFMT", separator=",", has_header=False,
                              new_columns=["ENTITY", "BRANCH1", "FMLOB", "FMPROD"])
        drfmt_df = drfmt_df.select(["BRANCH1", "FMLOB"])
    except:
        drfmt_df = pl.DataFrame({"BRANCH1": [], "FMLOB": []})
    
    # Process BRANCH1
    drfmt_df = drfmt_df.with_columns(
        pl.col("BRANCH1").cast(pl.Int64).alias("BRANCH2"),
        pl.col("BRANCH1").alias("BRANCX")
    ).rename({"BRANCH2": "BRANCH1"}).drop("BRANCX")
    
    # Read ELN data for all weeks
    eln_dfs = []
    for wk in [wk1, wk2, wk3, wk4]:
        try:
            df = pl.read_parquet(elds_path / f"ELN{reptmon}{wk}{reptyear}.parquet")
            df = df.with_columns(pl.lit("PBB ").alias("AGLFLD"))
            eln_dfs.append(df)
        except:
            pass
    
    eln_df = pl.concat(eln_dfs) if eln_dfs else pl.DataFrame()
    
    # Delete specific record
    if not eln_df.is_empty():
        eln_df = eln_df.filter(
            ~((pl.col("AANO") == "H18/512115/10") & (pl.col("AADATE") == 18329))
        )
    
    # Read IELN data for all weeks
    ieln_dfs = []
    for wk in [wk1, wk2, wk3, wk4]:
        try:
            df = pl.read_parquet(eldsi_path / f"IELN{reptmon}{wk}{reptyear}.parquet")
            df = df.with_columns(pl.lit("PIBB").alias("AGLFLD"))
            ieln_dfs.append(df)
        except:
            pass
    
    ieln_df = pl.concat(ieln_dfs) if ieln_dfs else pl.DataFrame()
    
    # Combine and filter - DIFFERENCE FROM 0963: STATUS='REJECTED' and different branch filter
    if not eln_df.is_empty() or not ieln_df.is_empty():
        elna_df = pl.concat([eln_df, ieln_df])
        elna_df = elna_df.filter(
            (pl.col("ADVANCES").is_in(["D", "X"])) &
            (
                (pl.col("STATUS") == "REJECTED BY BANK") |
                (pl.col("STATUS") == "REJECTED BY CUSTOMER(ACCEPTED)")
            ) &
            (pl.col("AMOUNT") > 0) &
            (~pl.col("BRANCH").is_in([902, 3902, 995, 3995, 3993]))
        )
    else:
        elna_df = pl.DataFrame()
    
    # Merge with LNFMT
    if not elna_df.is_empty() and not lnfmt_df.is_empty():
        elna_df = elna_df.sort("PRODUCT")
        lnfmt_df = lnfmt_df.sort("PRODUCT")
        elna_df = elna_df.join(lnfmt_df, on="PRODUCT", how="inner")
    
    # Split by LOB and merge with DRFMT (same as 0963)
    if not elna_df.is_empty():
        elna1_df = elna_df.filter(pl.col("LOB") == "PCCC")
        elna2_df = elna_df.filter(pl.col("LOB") != "PCCC")
        
        if not elna1_df.is_empty() and not drfmt_df.is_empty():
            elna1_df = elna1_df.sort("BRANCH1")
            drfmt_df = drfmt_df.sort("BRANCH1")
            elna1_df = elna1_df.join(drfmt_df, on="BRANCH1", how="left")
            elna1_df = elna1_df.with_columns(
                pl.coalesce(pl.col("FMLOB"), pl.col("LOB")).alias("LOB")
            ).drop("FMLOB")
        
        elna_df = pl.concat([elna1_df, elna2_df])
        
        # Adjust branch numbers and format BRANCX
        elna_df = elna_df.with_columns(
            pl.when(pl.col("BRANCH") > 3000)
            .then(pl.col("BRANCH") - 3000)
            .otherwise(pl.col("BRANCH"))
            .alias("BRANCH"),
            pl.when((pl.col("BRANCH") >= 800) & (pl.col("BRANCH") < 900))
            .then(pl.lit("H") + pl.col("BRANCH").cast(pl.Utf8))
            .otherwise(pl.lit("A") + pl.col("BRANCH").cast(pl.Utf8).str.zfill(3))
            .alias("BRANCX")
        )
    
    # Remove duplicates
    if not elna_df.is_empty():
        elna_df = elna_df.unique(["AANO", "AGLFLD"])
    
    # ELAPP processing - DIFFERENCE: No ORIBRCH handling for CORP LOB
    if not elna_df.is_empty():
        def apply_prodgrp_logic_0966(row):
            prodgrp = row.get("PRODGRP", "")
            brancx = row.get("BRANCX", "")
            producx = row.get("PRODUCX", "")
            lob = row.get("LOB", "")
            
            if prodgrp in ['2', '4']:
                if brancx.startswith('H') and producx == 'LSTAFF':
                    lob = 'HPOP'
            else:
                lob = 'RETL'
                if prodgrp == '1':
                    producx = 'LOT_OT '
                elif prodgrp == '3':
                    producx = 'LHOUSE '
                elif prodgrp == '5':
                    producx = 'LTF_OT '
                elif prodgrp == '6':
                    producx = 'LHPPRD '
                    lob = 'HPOP'
                elif prodgrp == '7':
                    producx = 'LOT_OT '
                elif prodgrp == '8':
                    producx = 'LOT_OT '
                elif prodgrp == '9':
                    producx = 'OOTHER '
                elif prodgrp == '':
                    producx = 'OOTHER '
            
            return lob, producx
        
        results = elna_df.select(["PRODGRP", "BRANCX", "PRODUCX", "LOB"]).map_rows(
            lambda row: apply_prodgrp_logic_0966(row)
        )
        
        elapp_df = elna_df.with_columns(
            pl.Series(results[:, 0]).alias("LOB"),
            pl.Series(results[:, 1]).alias("PRODUCX")
        )
    else:
        elapp_df = pl.DataFrame()
    
    # Summary statistics
    if not elapp_df.is_empty():
        elaps_df = elapp_df.group_by(["AGLFLD", "BRANCX", "PRODUCX", "LOB"]).agg([
            pl.sum("AMOUNT").alias("AMOUNT_SUM"),
            pl.count().alias("_FREQ_")
        ])
    else:
        elaps_df = pl.DataFrame()
    
    # Final processing - DIFFERENCE: CGLFLD='C_RAP' instead of 'C_AAP'
    if not elaps_df.is_empty():
        eln_df = elaps_df.with_columns(
            pl.when(pl.col("BRANCX").str.starts_with("A"))
            .then(pl.col("BRANCX").str.slice(1, 3))
            .otherwise(pl.col("BRANCX"))
            .alias("BRANCX"),
            pl.col("_FREQ_").alias("NOACCT"),
            pl.lit("EXT").alias("BGLFLD"),
            pl.lit("C_RAP").alias("CGLFLD"),  # DIFFERENT FROM 0963
            pl.lit("ACTUAL").alias("DGLFLD"),
            pl.lit(f"{ryear}{rmonth}").alias("YM")
        )
        
        # Add counters
        eln_df = eln_df.with_columns(
            pl.int_range(1, len(eln_df) + 1).alias("CNT")
        )
        numac = eln_df["NOACCT"].sum()
        
        # Write GLAPP file (same format as 0963)
        with open(base / "GLAPP.csv", 'w') as f:
            for idx, row in enumerate(eln_df.iter_rows(named=True)):
                line = (
                    f"D,{row['AGLFLD']:32s},{row['BGLFLD']:32s},"
                    f"{row['CGLFLD']:32s},{row['DGLFLD']:32s},,"
                    f"{row['BRANCX']:32s},{row['LOB']:32s},"
                    f"{row['PRODUCX']:32s},{row['YM']:6.0f},"
                    f"{row['NOACCT']:15.0f},N,,,"
                )
                f.write(line + "\n")
            
            # EOF record
            f.write(f"T,{len(eln_df):10.0f},{numac:15.0f},\n")
    
    print("EIBM0966 processing complete")

if __name__ == "__main__":
    eibm0966()
