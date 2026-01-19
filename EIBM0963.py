import polars as pl
from pathlib import Path
import sys

def eibm0963():
    base = Path.cwd()
    elds_path = base / "ELDS"
    eldsi_path = base / "ELDSI"
    
    # Execute external program PBBLNFMT
    try:
        from pbblnfmt import process as pbblnfmt_process
        pbblnfmt_process()
    except ImportError:
        print("Note: PGM(PBBLNFMT) not executed")
    
    # Step 1: REPTDATE processing
    reptdate_df = pl.read_parquet(elds_path / "REPTDATE.parquet")
    reptdate = reptdate_df["REPTDATE"][0]
    
    # SAS CALL SYMPUT equivalents
    wk1, wk2, wk3, wk4 = '1', '2', '3', '4'
    reptmon = f"{reptdate.month:02d}"
    rmonth = f"{reptdate.month:02d}"
    reptyear = str(reptdate.year)[-2:]
    ryear = str(reptdate.year)
    
    print(f"REPTMON: {reptmon}, RYEAR: {ryear}")
    
    # Step 2: Read LNFMT file (fixed-width)
    lnfmt_lines = []
    try:
        with open(base / "LNFMT", 'r') as f:
            for line in f:
                # @006 PRODUCT 3. @027 PRODUCX $7. @040 LOB $4.
                if len(line) >= 44:
                    product = line[5:8].strip()  # cols 6-8
                    producx = line[26:33].strip()  # cols 27-33
                    lob = line[39:43].strip()  # cols 40-43
                    lnfmt_lines.append({"PRODUCT": product, "PRODUCX": producx, "LOB": lob})
    except:
        lnfmt_lines = []
    
    lnfmt_df = pl.DataFrame(lnfmt_lines)
    lnfmt_df = lnfmt_df.unique(subset=["PRODUCT"], keep="first")
    
    # Step 3: Read DRFMT file (CSV)
    try:
        drfmt_df = pl.read_csv(base / "DRFMT", separator=",", has_header=False, 
                              new_columns=["ENTITY", "BRANCH1", "FMLOB", "FMPROD"])
        drfmt_df = drfmt_df.select(["BRANCH1", "FMLOB"])
    except:
        drfmt_df = pl.DataFrame({"BRANCH1": [], "FMLOB": []})
    
    # Process BRANCH1 conversion (character to numeric and back)
    drfmt_df = drfmt_df.with_columns(
        pl.col("BRANCH1").cast(pl.Int64).alias("BRANCH2"),
        pl.col("BRANCH1").alias("BRANCX")
    ).rename({"BRANCH2": "BRANCH1"}).drop("BRANCX")
    
    # Step 4: Read ELN data for all weeks
    eln_dfs = []
    for wk in [wk1, wk2, wk3, wk4]:
        try:
            df = pl.read_parquet(elds_path / f"ELN{reptmon}{wk}{reptyear}.parquet")
            df = df.with_columns(pl.lit("PBB ").alias("AGLFLD"))
            eln_dfs.append(df)
        except:
            pass
    
    if eln_dfs:
        eln_df = pl.concat(eln_dfs)
    else:
        eln_df = pl.DataFrame()
    
    # Delete specific record (SAS DELETE statement)
    if not eln_df.is_empty():
        eln_df = eln_df.filter(
            ~((pl.col("AANO") == "H18/512115/10") & (pl.col("AADATE") == 18329))
        )
    
    # Step 5: Read IELN data for all weeks
    ieln_dfs = []
    for wk in [wk1, wk2, wk3, wk4]:
        try:
            df = pl.read_parquet(eldsi_path / f"IELN{reptmon}{wk}{reptyear}.parquet")
            df = df.with_columns(pl.lit("PIBB").alias("AGLFLD"))
            ieln_dfs.append(df)
        except:
            pass
    
    if ieln_dfs:
        ieln_df = pl.concat(ieln_dfs)
    else:
        ieln_df = pl.DataFrame()
    
    # Step 6: Combine and filter ELNA
    if not eln_df.is_empty() or not ieln_df.is_empty():
        elna_df = pl.concat([eln_df, ieln_df])
        elna_df = elna_df.filter(
            (pl.col("ADVANCES").is_in(["D", "X"])) &
            (pl.col("STATUS") == "APPROVED") &
            (pl.col("AMOUNT") > 0) &
            (~pl.col("BRANCH").is_in([995, 3995, 3993]))
        )
        
        # Handle BRANCH 902, 3902 logic
        elna_df = elna_df.with_columns(
            pl.when(pl.col("BRANCH").is_in([902, 3902]))
            .then(pl.struct([
                pl.col("BRANCH").alias("ORIBRCH"),
                pl.col("TRANBRNO").alias("BRANCH")
            ]))
            .otherwise(pl.struct([
                pl.lit(None).alias("ORIBRCH"),
                pl.col("BRANCH")
            ]))
            .alias("temp")
        ).unnest("temp")
    else:
        elna_df = pl.DataFrame()
    
    # Step 7: Merge with LNFMT
    if not elna_df.is_empty() and not lnfmt_df.is_empty():
        elna_df = elna_df.sort("PRODUCT")
        lnfmt_df = lnfmt_df.sort("PRODUCT")
        elna_df = elna_df.join(lnfmt_df, on="PRODUCT", how="inner")
    
    # Step 8: Split by LOB and merge with DRFMT
    if not elna_df.is_empty():
        elna1_df = elna_df.filter(pl.col("LOB") == "PCCC")
        elna2_df = elna_df.filter(pl.col("LOB") != "PCCC")
        
        if not elna1_df.is_empty() and not drfmt_df.is_empty():
            elna1_df = elna1_df.sort("BRANCH1")
            drfmt_df = drfmt_df.sort("BRANCH1")
            elna1_df = elna1_df.join(drfmt_df, on="BRANCH1", how="left", suffix="_dr")
            elna1_df = elna1_df.with_columns(
                pl.coalesce(pl.col("FMLOB"), pl.col("LOB")).alias("LOB")
            ).drop("FMLOB")
        
        # Recombine
        elna_df = pl.concat([elna1_df, elna2_df])
        
        # Adjust branch numbers
        elna_df = elna_df.with_columns(
            pl.when(pl.col("BRANCH") > 3000)
            .then(pl.col("BRANCH") - 3000)
            .otherwise(pl.col("BRANCH"))
            .alias("BRANCH")
        )
        
        # Format BRANCX
        elna_df = elna_df.with_columns(
            pl.when((pl.col("BRANCH") >= 800) & (pl.col("BRANCH") < 900))
            .then(pl.lit("H") + pl.col("BRANCH").cast(pl.Utf8))  # Simplified HPCC format
            .otherwise(pl.lit("A") + pl.col("BRANCH").cast(pl.Utf8).str.zfill(3))
            .alias("BRANCX")
        )
    
    # Step 9: Remove duplicates
    if not elna_df.is_empty():
        elna_df = elna_df.unique(subset=["AANO", "AGLFLD"], keep="first")
    
    # Step 10: ELAPP processing (PRODGRP logic)
    if not elna_df.is_empty():
        elapp_df = elna_df
        
        # Apply PRODGRP logic
        def apply_prodgrp_logic(row):
            prodgrp = row.get("PRODGRP", "")
            brancx = row.get("BRANCX", "")
            producx = row.get("PRODUCX", "")
            lob = row.get("LOB", "")
            oribrch = row.get("ORIBRCH", None)
            
            # Initialize
            new_lob = lob
            new_producx = producx
            
            if prodgrp in ['2', '4']:
                if brancx.startswith('H') and producx == 'LSTAFF':
                    new_lob = 'HPOP'
            else:
                new_lob = 'RETL'
                if prodgrp == '1':
                    new_producx = 'LOT_OT '
                elif prodgrp == '3':
                    new_producx = 'LHOUSE '
                elif prodgrp == '5':
                    new_producx = 'LTF_OT '
                elif prodgrp == '6':
                    new_producx = 'LHPPRD '
                    new_lob = 'HPOP'
                elif prodgrp == '7':
                    new_producx = 'LOT_OT '
                elif prodgrp == '8':
                    new_producx = 'LOT_OT '
                elif prodgrp == '9':
                    new_producx = 'OOTHER '
                elif prodgrp == '':
                    new_producx = 'OOTHER '
            
            if oribrch in [902, 3902]:
                new_lob = 'CORP'
            
            return new_lob, new_producx
        
        # Apply the logic
        results = elapp_df.select(["PRODGRP", "BRANCX", "PRODUCX", "LOB", "ORIBRCH"]).map_rows(
            lambda row: apply_prodgrp_logic(row)
        )
        
        elapp_df = elapp_df.with_columns(
            pl.Series(results[:, 0]).alias("LOB"),
            pl.Series(results[:, 1]).alias("PRODUCX")
        )
    
    # Step 11: Summary statistics
    if not elapp_df.is_empty():
        elaps_df = elapp_df.group_by(["AGLFLD", "BRANCX", "PRODUCX", "LOB"]).agg([
            pl.sum("AMOUNT").alias("AMOUNT_SUM"),
            pl.count().alias("_FREQ_")
        ])
    else:
        elaps_df = pl.DataFrame()
    
    # Step 12: Final ELN processing
    if not elaps_df.is_empty():
        eln_df = elaps_df.with_columns(
            pl.when(pl.col("BRANCX").str.starts_with("A"))
            .then(pl.col("BRANCX").str.slice(1, 3))
            .otherwise(pl.col("BRANCX"))
            .alias("BRANCX"),
            pl.col("_FREQ_").alias("NOACCT"),
            pl.lit("EXT").alias("BGLFLD"),
            pl.lit("C_AAP").alias("CGLFLD"),
            pl.lit("ACTUAL").alias("DGLFLD"),
            pl.lit(f"{ryear}{rmonth}").alias("YM")
        )
        
        # Add counters
        eln_df = eln_df.with_columns(
            pl.int_range(1, len(eln_df) + 1).alias("CNT")
        )
        numac = eln_df["NOACCT"].sum()
        
        # Write GLAPP file with fixed positions
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
            
            # EOF record (T record)
            f.write(f"T,{len(eln_df):10.0f},{numac:15.0f},\n")
    
    print("EIBM0963 processing complete")

if __name__ == "__main__":
    eibm0963()
