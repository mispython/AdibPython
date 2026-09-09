import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
import pyreadstat
from NPGSRPT import npgs_report

def read_sas_dataset(file_path):
    """Read SAS dataset and convert to Polars DataFrame with lowercase column names"""
    try:
        df, meta = pyreadstat.read_sas7bdat(str(file_path))
        pl_df = pl.from_pandas(df)
        # Convert column names to lowercase
        pl_df = pl_df.rename({col: col.lower() for col in pl_df.columns})
        return pl_df
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return pl.DataFrame()

def ensure_common_columns(dfs, required_columns):
    """Ensure all DataFrames have the required columns"""
    result_dfs = []
    for df in dfs:
        if df.is_empty():
            continue
        # Add missing columns with None
        for col in required_columns:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))
        # Select only required columns
        df = df.select(required_columns)
        result_dfs.append(df)
    return result_dfs

def eibsnpgs():
    npgs_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBSNPGS/NPGS")
    npgsi_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBSNPGS/NPGSI")
    output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBSNPGS")
    
    # Create output directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Calculate report date (yesterday)
    reptdate = datetime.now() - timedelta(days=1)
    
    mm = reptdate.month
    mm1 = mm - 1 if mm > 1 else 12
    
    reptmon = f"{mm:02d}"
    reptmon1 = f"{mm1:02d}"
    reptyear = str(reptdate.year)
    reptday = f"{reptdate.day:02d}"
    rdate = reptdate.strftime("%d%m%y")
    ndate = f"{reptdate.day:02d}{reptdate.month:02d}"
    
    print(f"REPTMON: {reptmon}, RDATE: {rdate}")
    print(f"NPGS Path: {npgs_path}")
    print(f"NPGSI Path: {npgsi_path}")
    print(f"Output Path: {output_path}")
    
    # Define required columns for output
    required_cols = ['cvar01','cvar02','cvar03','cvar04','cvar05','cvar06',
                    'cvar07','cvar08','cvar09','cvar10','cvar11','cvar12',
                    'cvar13','cvar14','cvar15','cvarxx','natguar','cinstcl']
    
    # Helper function to read datasets
    def read_dataset(path, file_name):
        file_path = path / file_name
        if file_path.exists():
            df = read_sas_dataset(file_path)
            if not df.is_empty():
                print(f"Read {file_name}: {len(df)} records, columns: {df.columns}")
            return df
        else:
            print(f"File not found: {file_path}")
            return pl.DataFrame()
    
    # 1. SC53: BTNPGS + LNNPGS + DPNPGS
    print("\nProcessing SC53 datasets...")
    bt_df = read_dataset(npgs_path, f"btnpgs{reptmon}.sas7bdat")
    ln_df = read_dataset(npgs_path, f"lnnpgs{reptmon}.sas7bdat")
    dp_df = read_dataset(npgs_path, f"dpnpgs{reptmon}.sas7bdat")
    
    # Ensure common columns before concatenation
    sc53_dfs = ensure_common_columns([bt_df, ln_df, dp_df], required_cols)
    
    if sc53_dfs:
        sc53_df = pl.concat(sc53_dfs)
        sc53_df = sc53_df.filter(
            (pl.col("cvar02") == "53") &
            (pl.col("natguar") == "06") &
            (pl.col("cinstcl") == "18")
        )
        sc53_df = sc53_df.with_columns([
            pl.lit(" " * 10).alias("cvarxx"),
            pl.when(pl.col("cvar11") < 3).then(pl.lit("   ")).otherwise(pl.col("cvar12")).alias("cvar12"),
            pl.lit("E1").alias("cvar02")
        ])
        sc53_df = sc53_df.sort(["cvar01", "cvar06"])
        print(f"SC53 records: {len(sc53_df)}")
    else:
        sc53_df = pl.DataFrame()
        print("SC53: No data")
    
    # 2. SCEI: DPNPGS + LNIPGS (Islamic)
    print("\nProcessing SCEI datasets...")
    dp_i_df = read_dataset(npgsi_path, f"dpnpgs{reptmon}.sas7bdat")
    ln_i_df = read_dataset(npgsi_path, f"lnipgs{reptmon}.sas7bdat")
    
    # Ensure common columns before concatenation
    scei_dfs = ensure_common_columns([dp_i_df, ln_i_df], required_cols)
    
    if scei_dfs:
        scei_df = pl.concat(scei_dfs)
        scei_df = scei_df.filter(
            (pl.col("natguar") == "06") &
            (pl.col("cinstcl") == "18")
        )
        scei_df = scei_df.with_columns([
            pl.when(pl.col("cvar12") == "NPL").then(pl.lit("NP")).otherwise(pl.lit("AP")).alias("cvar12"),
            pl.lit(" " * 10).alias("cvarxx"),
            pl.lit("E2").alias("cvar02")
        ])
        scei_df = scei_df.sort(["cvar01", "cvar06"])
        print(f"SCEI records: {len(scei_df)}")
    else:
        scei_df = pl.DataFrame()
        print("SCEI: No data")
    
    # 3. OTH: LNNPGS with complex conditional logic
    print("\nProcessing OTH datasets...")
    oth_df = read_dataset(npgs_path, f"lnnpgs{reptmon}.sas7bdat")
    if not oth_df.is_empty():
        # Ensure required columns exist
        for col in required_cols:
            if col not in oth_df.columns:
                oth_df = oth_df.with_columns(pl.lit(None).alias(col))
        
        oth_df = oth_df.filter(
            pl.col("cvar02").is_in(['81','2Z','4Z','H4','H5','H6','H7','F5','F6',
                                   '1Z','3Z','5S','6S','1H','2H','3H','4H','E6',
                                   '5Z','5H','6H']) &
            (pl.col("natguar") == "06") &
            (pl.col("cinstcl") == "18")
        )
        oth_df = oth_df.with_columns(pl.lit(" " * 10).alias("cvarxx"))
        
        # Store original cvar02 for conditional logic
        oth_df = oth_df.with_columns(pl.col("cvar02").alias("orig_cvar02"))
        
        # Apply conditional logic exactly as SAS code
        # IF CVAR02 = '81'
        oth_df = oth_df.with_columns([
            # cvar02 = 'G1' for '81'
            pl.when(pl.col("cvar02") == "81")
            .then(pl.lit("G1"))
            .otherwise(pl.col("cvar02"))
            .alias("cvar02"),
            # IF CVAR11 < 3 THEN CVAR12='   '
            pl.when((pl.col("orig_cvar02") == "81") & (pl.col("cvar11") < 3))
            .then(pl.lit("   "))
            .otherwise(pl.col("cvar12"))
            .alias("cvar12")
        ])
        
        # ELSE IF CVAR02 IN ('2Z','4Z','F6')
        oth_df = oth_df.with_columns([
            pl.when(pl.col("orig_cvar02").is_in(['2Z','4Z','F6']))
            .then(pl.lit("TF"))
            .otherwise(pl.col("cvar07"))
            .alias("cvar07"),
            pl.when(pl.col("orig_cvar02").is_in(['2Z','4Z','F6']) & (pl.col("cvar11") < 3))
            .then(pl.lit("   "))
            .when(pl.col("orig_cvar02").is_in(['2Z','4Z','F6']))
            .then(pl.lit("NPF"))
            .otherwise(pl.col("cvar12"))
            .alias("cvar12")
        ])
        
        # ELSE IF CVAR02 = 'H4' (first condition)
        oth_df = oth_df.with_columns([
            pl.when(pl.col("orig_cvar02") == "H4")
            .then(pl.lit("TL"))
            .otherwise(pl.col("cvar07"))
            .alias("cvar07"),
            pl.when((pl.col("orig_cvar02") == "H4") & (pl.col("cvar11") < 3))
            .then(pl.lit("   "))
            .when((pl.col("orig_cvar02") == "H4") & (pl.col("cvar12") == " "))
            .then(pl.lit("AP"))
            .otherwise(pl.col("cvar12"))
            .alias("cvar12")
        ])
        
        # ELSE IF CVAR02 = 'H5'
        oth_df = oth_df.with_columns([
            pl.when(pl.col("orig_cvar02") == "H5")
            .then(pl.lit("TF"))
            .otherwise(pl.col("cvar07"))
            .alias("cvar07"),
            pl.when((pl.col("orig_cvar02") == "H5") & (pl.col("cvar11") < 3))
            .then(pl.lit("   "))
            .when((pl.col("orig_cvar02") == "H5") & (pl.col("cvar12") == " "))
            .then(pl.lit("AP"))
            .otherwise(pl.col("cvar12"))
            .alias("cvar12")
        ])
        
        # ELSE IF CVAR02 = 'H6'
        oth_df = oth_df.with_columns([
            pl.when(pl.col("orig_cvar02") == "H6")
            .then(pl.lit("FL"))
            .otherwise(pl.col("cvar07"))
            .alias("cvar07"),
            pl.when((pl.col("orig_cvar02") == "H6") & (pl.col("cvar11") < 3))
            .then(pl.lit("   "))
            .otherwise(pl.col("cvar12"))
            .alias("cvar12"),
            pl.when((pl.col("orig_cvar02") == "H6") & (pl.col("cvar12") == "   "))
            .then(pl.lit("          "))
            .otherwise(pl.col("cvar13"))
            .alias("cvar13")
        ])
        
        # ELSE IF CVAR02 = 'H7'
        oth_df = oth_df.with_columns([
            pl.when(pl.col("orig_cvar02") == "H7")
            .then(pl.lit("TF"))
            .otherwise(pl.col("cvar07"))
            .alias("cvar07"),
            pl.when((pl.col("orig_cvar02") == "H7") & (pl.col("cvar11") < 3))
            .then(pl.lit("   "))
            .otherwise(pl.col("cvar12"))
            .alias("cvar12"),
            pl.when((pl.col("orig_cvar02") == "H7") & (pl.col("cvar12") == "   "))
            .then(pl.lit("          "))
            .otherwise(pl.col("cvar13"))
            .alias("cvar13")
        ])
        
        # ELSE IF CVAR02 = 'F5'
        oth_df = oth_df.with_columns([
            pl.when(pl.col("orig_cvar02") == "F5")
            .then(pl.lit("FL"))
            .otherwise(pl.col("cvar07"))
            .alias("cvar07"),
            pl.when((pl.col("orig_cvar02") == "F5") & (pl.col("cvar11") < 3))
            .then(pl.lit("   "))
            .otherwise(pl.col("cvar12"))
            .alias("cvar12")
        ])
        
        # ELSE IF CVAR02 IN ('1Z','3Z','5S','1H','3H')
        oth_df = oth_df.with_columns([
            pl.when(pl.col("orig_cvar02").is_in(['1Z','3Z','5S','1H','3H']))
            .then(pl.lit("FL"))
            .otherwise(pl.col("cvar07"))
            .alias("cvar07"),
            pl.when(pl.col("orig_cvar02").is_in(['1Z','3Z','5S','1H','3H']) & (pl.col("cvar11") < 3))
            .then(pl.lit("   "))
            .when(pl.col("orig_cvar02").is_in(['1Z','3Z','5S','1H','3H']))
            .then(pl.lit("NPL"))
            .otherwise(pl.col("cvar12"))
            .alias("cvar12")
        ])
        
        # ELSE IF CVAR02 = '6S'
        oth_df = oth_df.with_columns([
            pl.when(pl.col("orig_cvar02") == "6S")
            .then(pl.lit("TF"))
            .otherwise(pl.col("cvar07"))
            .alias("cvar07"),
            pl.when((pl.col("orig_cvar02") == "6S") & (pl.col("cvar11") < 3))
            .then(pl.lit("   "))
            .when(pl.col("orig_cvar02") == "6S")
            .then(pl.lit("NPL"))
            .otherwise(pl.col("cvar12"))
            .alias("cvar12")
        ])
        
        # ELSE IF CVAR02 IN ('2H','4H')
        oth_df = oth_df.with_columns([
            pl.when(pl.col("orig_cvar02").is_in(['2H','4H']))
            .then(pl.lit("TL"))
            .otherwise(pl.col("cvar07"))
            .alias("cvar07"),
            pl.when(pl.col("orig_cvar02").is_in(['2H','4H']) & (pl.col("cvar11") < 3))
            .then(pl.lit("   "))
            .when(pl.col("orig_cvar02").is_in(['2H','4H']))
            .then(pl.lit("NPF"))
            .otherwise(pl.col("cvar12"))
            .alias("cvar12")
        ])
        
        # ELSE IF CVAR02 IN ('E6','5Z','5H','6H')
        oth_df = oth_df.with_columns([
            pl.when(pl.col("orig_cvar02").is_in(['E6','5Z','5H','6H']) & (pl.col("cvar11") < 3))
            .then(pl.lit("   "))
            .otherwise(pl.col("cvar12"))
            .alias("cvar12"),
            pl.when(pl.col("orig_cvar02").is_in(['E6','5Z','5H','6H']) & (pl.col("cvar12") == "   "))
            .then(pl.lit("          "))
            .otherwise(pl.col("cvar13"))
            .alias("cvar13")
        ])
        
        # Drop the temporary column
        oth_df = oth_df.drop("orig_cvar02")
        oth_df = oth_df.sort(["cvar01", "cvar06"])
        print(f"OTH records: {len(oth_df)}")
    else:
        oth_df = pl.DataFrame()
        print("OTH: No data")
    
    # 4. Combine all datasets (SC53 SCEI OTH)
    print("\nCombining all datasets...")
    
    # Define final output columns
    final_cols = ['cvar01','cvar02','cvar03','cvar04','cvar05','cvar06',
                 'cvar07','cvar08','cvar09','cvar10','cvar11','cvar12',
                 'cvar13','cvar14','cvar15','cvarxx']
    
    all_dfs = []
    for df_name, df in [("SC53", sc53_df), ("SCEI", scei_df), ("OTH", oth_df)]:
        if not df.is_empty():
            # Ensure all final columns exist
            for col in final_cols:
                if col not in df.columns:
                    df = df.with_columns(pl.lit(None).alias(col))
            # Select only final columns
            df = df.select(final_cols)
            all_dfs.append(df)
            print(f"{df_name}: {len(df)} records")
    
    if not all_dfs:
        print("No data found in any source")
        return
    
    npgs_df = pl.concat(all_dfs)
    npgs_df = npgs_df.sort(["cvar02", "cvar01", "cvar06"])
    print(f"Total NPGS records: {len(npgs_df)}")
    
    # 5. Write COMBT file (DSD DLM=';')
    # Add LASTCOL
    npgs_df = npgs_df.with_columns(pl.lit("").alias("lastcol"))
    
    output_cols = final_cols + ['lastcol']
    
    print("\nWriting COMBT.txt...")
    with open(output_path / "COMBT.txt", "w") as f:
        for row in npgs_df.iter_rows(named=True):
            values = []
            for col in output_cols:
                val = row.get(col)
                if val is None:
                    values.append("")
                elif isinstance(val, (int, float)):
                    if col == 'cvar05':  # Date field
                        if val > 0:
                            date_val = datetime(1960, 1, 1) + timedelta(days=int(val))
                            values.append(date_val.strftime("%d/%m/%Y"))
                        else:
                            values.append("")
                    else:
                        values.append(str(val))
                else:
                    values.append(str(val).strip())
            f.write(";".join(values) + "\n")
    
    # 6. Generate report using NPGSRPT module
    print("\n" + "=" * 60)
    print("PUBLIC BANK BERHAD")
    print(f"DETAIL OF ACCTS NON-PG FOR SUBMISSION TO CGC @ {rdate}")
    print("=" * 60)
    
    # Use shared report module
    title1 = "PUBLIC BANK BERHAD"
    title2 = f"DETAIL OF ACCTS NON-PG FOR SUBMISSION TO CGC @ {rdate}"
    
    npgs_report(
        df=npgs_df,
        report_path=str(output_path / "COMBR.txt"),
        title1=title1,
        title2=title2
    )
    
    print(f"\nProcessing complete. Files: COMBT.txt, COMBR.txt")
    print(f"Output directory: {output_path}")
    print(f"Total records: {len(npgs_df)}")
    if 'cvar02' in npgs_df.columns:
        counts = npgs_df.group_by("cvar02").agg(pl.count().alias("records"))
        print("\nRecords by CVAR02:")
        for row in counts.iter_rows(named=True):
            print(f"  {row['cvar02']}: {row['records']}")

if __name__ == "__main__":
    eibsnpgs()
