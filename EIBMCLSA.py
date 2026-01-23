import polars as pl
import duckdb
from pathlib import Path
import datetime

# Configure input file paths
input_path = Path("/path/to/input")  # Update with your actual path
output_path = Path("/path/to/output")  # Update with your actual path

def load_reptdate() -> tuple[str, str, str]:
    """Load REPTDATE data and extract year/month using Polars"""
    # Read REPTDATE - adjust format based on your file type
    reptdate_df = pl.read_csv(input_path / "DEP/REPTDATE.csv")
    
    # Parse date and calculate MM1 based on SAS logic
    reptdate_df = reptdate_df.with_columns([
        pl.col("REPTDATE").str.strptime(pl.Date, format="%Y-%m-%d").alias("reptdate_parsed"),
        pl.col("REPTDATE").str.strptime(pl.Date, format="%Y-%m-%d").dt.month().alias("MM")
    ]).with_columns([
        # Check if next day is 1st of month
        (pl.col("reptdate_parsed") + datetime.timedelta(days=1)).dt.day().alias("next_day")
    ]).with_columns([
        # Calculate MM1: if next day is 1, MM1 = MM, else MM1 = MM-1
        pl.when(pl.col("next_day") == 1)
        .then(pl.col("MM"))
        .otherwise(pl.col("MM") - 1)
        .alias("MM1_temp")
    ]).with_columns([
        # Adjust MM1 if 0 to 12
        pl.when(pl.col("MM1_temp") == 0)
        .then(pl.lit(12))
        .otherwise(pl.col("MM1_temp"))
        .alias("MM1")
    ])
    
    # Get values from first row
    first_row = reptdate_df.row(0)
    REPTYEAR = str(first_row[0].year)[-2:]  # Last 2 digits of year
    REPTMON = f"{first_row[1]:02d}"  # Month with leading zero
    REPTMON1 = f"{first_row[2]:02d}"  # MM1 with leading zero
    
    return REPTYEAR, REPTMON, REPTMON1

def process_cis_with_polars() -> pl.DataFrame:
    """Process CIS data using Polars (excellent for transformations)"""
    # Read CIS deposit data
    cis_df = pl.read_csv(input_path / "CIS/DEPOSIT.csv")
    
    # Filter SECCUST = '901' and create date columns
    cis_df = (
        cis_df
        .filter(pl.col("SECCUST") == "901")
        .with_columns([
            # Extract date components
            pl.col("BIRTHDAT").str.slice(0, 2).cast(pl.Int32).alias("BDD"),
            pl.col("BIRTHDAT").str.slice(2, 2).cast(pl.Int32).alias("BMM"),
            pl.col("BIRTHDAT").str.slice(4, 4).cast(pl.Int32).alias("BYY"),
            # Alias CITIZEN to COUNTRY
            pl.col("CITIZEN").alias("COUNTRY")
        ])
        .with_columns([
            # Create DOBCIS date
            pl.date(pl.col("BYY"), pl.col("BMM"), pl.col("BDD")).alias("DOBCIS")
        ])
        # Keep only required columns
        .select(["ACCTNO", "DOBCIS", "OCCUPAT", "COUNTRY", "RACE"])
        # Remove duplicates (similar to NODUPKEY)
        .unique(subset=["ACCTNO"], keep="first")
    )
    
    return cis_df

def monthly_processing_with_polars(reptmon: str, reptmon1: str) -> pl.DataFrame:
    """Handle monthly processing logic using Polars"""
    # Load SAVG data
    savg_df = pl.read_csv(input_path / f"MIS/SAVGC{reptmon}.csv").sort("ACCTNO")
    
    # Check conditions similar to SAS macro
    if reptmon != reptmon1 and reptmon == "01":
        # Initialize FEEYTD to 0
        savg_df = savg_df.with_columns(pl.lit(0).alias("FEEYTD"))
    else:
        # Load and merge CRMSA data
        crmsa_df = (
            pl.read_csv(input_path / f"MNICRM/SA{reptmon1}.csv")
            .unique(subset=["ACCTNO"], keep="first")
            .drop("COSTCTR")
        )
        
        # Merge with SAVG
        savg_df = savg_df.join(crmsa_df, on="ACCTNO", how="inner")
    
    return savg_df

def transform_dates_with_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Transform OPENDT and CLOSEDT using Polars (excellent for this)"""
    def parse_sas_date(date_col):
        """Parse SAS-style date (z11 format)"""
        return (
            date_col.cast(pl.Utf8)
            .str.zfill(11)
            .str.slice(0, 8)
            .str.strptime(pl.Date, format="%m%d%Y")
        )
    
    df = df.with_columns([
        # Parse OPENDT if > 0
        pl.when(pl.col("OPENDT") > 0)
        .then(parse_sas_date(pl.col("OPENDT")))
        .alias("ODATE"),
        
        # Parse CLOSEDT if > 0
        pl.when(pl.col("CLOSEDT") > 0)
        .then(parse_sas_date(pl.col("CLOSEDT")))
        .alias("CDATE"),
        
        # Handle ESIGNATURE
        pl.when(pl.col("ESIGNATURE") == "")
        .then(pl.lit("N"))
        .otherwise(pl.col("ESIGNATURE"))
        .alias("ESIGNATURE")
    ]).drop(["OPENDT", "CLOSEDT"])
    
    return df

def process_dpclos_with_polars() -> pl.DataFrame:
    """Process DPCLOS data - keep latest and remove duplicates"""
    # Load DPCLOS data
    dpclos_df = pl.read_csv(input_path / "PRODEV/DPCLOS.csv")
    
    # Keep latest by descending CLOSEDT and remove duplicates
    dpclos_df = (
        dpclos_df
        .sort("CLOSEDT", descending=True)
        .unique(subset=["ACCTNO"], keep="first")
        .select(["ACCTNO", "CLOSEDT", "RCODE"])
        .sort("ACCTNO")
    )
    
    return dpclos_df

def merge_with_duckdb(
    cis_df: pl.DataFrame,
    savg_df: pl.DataFrame,
    saacc_df: pl.DataFrame,
    dpclos_df: pl.DataFrame,
    reptmon: str,
    reptyear: str
) -> pl.DataFrame:
    """
    Use DuckDB for complex SAS-style merges
    This mirrors the SAS DATA step merge logic
    """
    # Create DuckDB connection
    conn = duckdb.connect()
    
    # Register Polars DataFrames with DuckDB
    conn.register("cis", cis_df)
    conn.register("savg", savg_df)
    conn.register("saacc", saacc_df)
    conn.register("dpclos", dpclos_df)
    
    # Load MISMTD.SAVG data directly using DuckDB's file reading
    conn.execute(f"""
        CREATE OR REPLACE TEMP VIEW mis_mtd AS
        SELECT * 
        FROM read_csv('{input_path}/MISMTD/SAVG{reptmon}.csv')
        ORDER BY ACCTNO
    """)
    
    # Execute the main merge (mirrors SAS DATA step merge)
    # This is where DuckDB shines for complex joins
    result = conn.execute("""
        -- First merge: CIS + SAVG + SAACC (similar to DDWH in SAS)
        WITH ddwh AS (
            SELECT 
                cis.ACCTNO,
                cis.DOBCIS,
                cis.OCCUPAT,
                cis.COUNTRY,
                cis.RACE,
                savg.* EXCLUDE (ACCTNO),
                saacc.* EXCLUDE (ACCTNO)
            FROM cis
            INNER JOIN savg ON cis.ACCTNO = savg.ACCTNO
            LEFT JOIN saacc ON cis.ACCTNO = saacc.ACCTNO
        ),
        
        -- Create SACLOSE with date renaming
        saclose AS (
            SELECT 
                * EXCLUDE (OPENDT, CLOSEDT),
                ODATE AS OPENDT,
                CDATE AS CLOSEDT
            FROM ddwh
        )
        
        -- Final merge: SACLOSE + DPCLOS + MISMTD.SAVG
        SELECT 
            s.*,
            dpclos.CLOSEDT AS DPCLOS_CLOSEDT,
            dpclos.RCODE,
            mis_mtd.* EXCLUDE (ACCTNO)
        FROM saclose s
        LEFT JOIN dpclos ON s.ACCTNO = dpclos.ACCTNO
        LEFT JOIN mis_mtd ON s.ACCTNO = mis_mtd.ACCTNO
        ORDER BY s.ACCTNO
    """).pl()  # Convert back to Polars DataFrame
    
    conn.close()
    return result

def alternative_polars_only_version():
    """Version using only Polars (simpler but less SQL-like)"""
    REPTYEAR, REPTMON, REPTMON1 = load_reptdate()
    
    # Process CIS data
    cis_df = process_cis_with_polars()
    
    # Monthly processing
    savg_df = monthly_processing_with_polars(REPTMON, REPTMON1)
    
    # Load other datasets
    saacc_df = pl.read_csv(input_path / "SIGNA/SMSACC.csv").sort("ACCTNO")
    mis_mtd_df = pl.read_csv(input_path / f"MISMTD/SAVG{REPTMON}.csv")
    
    # Process DPCLOS
    dpclos_df = process_dpclos_with_polars()
    
    # Merge CIS, SAVG, SAACC (DDWH)
    ddwh_df = (
        cis_df
        .join(savg_df, on="ACCTNO", how="inner")
        .join(saacc_df, on="ACCTNO", how="left")
    )
    
    # Transform dates
    ddwh_df = transform_dates_with_polars(ddwh_df)
    
    # Create SACLOSE with renamed columns
    saclose_df = ddwh_df.with_columns([
        pl.col("ODATE").alias("OPENDT"),
        pl.col("CDATE").alias("CLOSEDT")
    ]).drop(["ODATE", "CDATE"])
    
    # Final merge with DPCLOS and MISMTD.SAVG
    final_df = (
        saclose_df
        .join(dpclos_df, on="ACCTNO", how="left", suffix="_DPCLOS")
        .join(mis_mtd_df, on="ACCTNO", how="left")
    )
    
    # Save result
    output_file = output_path / f"SACLOSE{REPTMON}{REPTYEAR}_polars.parquet"
    final_df.write_parquet(output_file)
    
    return final_df

def main():
    """Main processing function using hybrid approach"""
    print("Starting EIBMCLSA processing...")
    
    # Step 1: Get report date parameters (Polars)
    REPTYEAR, REPTMON, REPTMON1 = load_reptdate()
    print(f"Processing for month: {REPTMON}, year: {REPTYEAR}, previous month: {REPTMON1}")
    
    # Step 2: Process CIS data (Polars)
    print("Processing CIS data...")
    cis_df = process_cis_with_polars()
    print(f"CIS data shape: {cis_df.shape}")
    
    # Step 3: Monthly processing (Polars)
    print("Processing monthly SAVG data...")
    savg_df = monthly_processing_with_polars(REPTMON, REPTMON1)
    print(f"SAVG data shape: {savg_df.shape}")
    
    # Step 4: Load SAACC data (Polars)
    print("Loading SAACC data...")
    saacc_df = pl.read_csv(input_path / "SIGNA/SMSACC.csv").sort("ACCTNO")
    
    # Step 5: Process DPCLOS data (Polars)
    print("Processing DPCLOS data...")
    dpclos_df = process_dpclos_with_polars()
    print(f"DPCLOS data shape: {dpclos_df.shape}")
    
    # Step 6: Complex merges (DuckDB)
    print("Merging datasets with DuckDB...")
    final_df = merge_with_duckdb(
        cis_df, savg_df, saacc_df, dpclos_df, REPTMON, REPTYEAR
    )
    print(f"Final data shape: {final_df.shape}")
    
    # Step 7: Save to Parquet (Polars)
    output_file = output_path / f"SACLOSE{REPTMON}{REPTYEAR}.parquet"
    final_df.write_parquet(output_file)
    
    # Also save metadata
    metadata_file = output_path / f"SACLOSE{REPTMON}{REPTYEAR}_metadata.txt"
    with open(metadata_file, 'w') as f:
        f.write(f"Records: {len(final_df)}\n")
        f.write(f"Columns: {len(final_df.columns)}\n")
        f.write(f"Generated: {datetime.datetime.now()}\n")
        f.write(f"Month: {REPTMON}\n")
        f.write(f"Year: {REPTYEAR}\n")
        f.write(f"Previous Month: {REPTMON1}\n")
    
    print(f"✅ Processing complete!")
    print(f"   Output saved to: {output_file}")
    print(f"   Records processed: {len(final_df):,}")
    print(f"   Columns: {len(final_df.columns)}")
    
    # Show sample
    print("\nSample data (first 5 rows):")
    print(final_df.select(["ACCTNO", "DOBCIS", "OPENDT", "RCODE"]).head())
    
    return final_df

def run_hybrid_optimized():
    """Optimized hybrid version using each tool's strengths"""
    REPTYEAR, REPTMON, REPTMON1 = load_reptdate()
    
    # Create DuckDB connection early for all operations
    conn = duckdb.connect()
    
    # Load REPTDATE once and create macros as variables
    print(f"Processing for {REPTMON}/{REPTYEAR}, previous: {REPTMON1}")
    
    # 1. Load and process CIS with DuckDB directly
    cis_df = conn.execute(f"""
        WITH deposit_data AS (
            SELECT 
                ACCTNO,
                CITIZEN AS COUNTRY,
                OCCUPAT,
                RACE,
                SUBSTR(BIRTHDAT, 1, 2)::INTEGER AS BDD,
                SUBSTR(BIRTHDAT, 3, 2)::INTEGER AS BMM,
                SUBSTR(BIRTHDAT, 5, 4)::INTEGER AS BYY
            FROM read_csv('{input_path}/CIS/DEPOSIT.csv')
            WHERE SECCUST = '901'
        ),
        cis_processed AS (
            SELECT 
                ACCTNO,
                make_date(BYY, BMM, BDD) AS DOBCIS,
                OCCUPAT,
                COUNTRY,
                RACE
            FROM deposit_data
        )
        SELECT DISTINCT ACCTNO, DOBCIS, OCCUPAT, COUNTRY, RACE
        FROM cis_processed
        ORDER BY ACCTNO
    """).pl()
    
    conn.register("cis", cis_df)
    
    # 2. Load SAVG with monthly processing
    if REPTMON != REPTMON1 and REPTMON == "01":
        savg_df = conn.execute(f"""
            SELECT *, 0 AS FEEYTD
            FROM read_csv('{input_path}/MIS/SAVGC{REPTMON}.csv')
            ORDER BY ACCTNO
        """).pl()
    else:
        savg_df = conn.execute(f"""
            WITH savg_base AS (
                SELECT * FROM read_csv('{input_path}/MIS/SAVGC{REPTMON}.csv')
            ),
            crmsa_base AS (
                SELECT DISTINCT ACCTNO, * EXCLUDE (ACCTNO, COSTCTR)
                FROM read_csv('{input_path}/MNICRM/SA{REPTMON1}.csv')
            )
            SELECT 
                s.*,
                c.* EXCLUDE (ACCTNO)
            FROM savg_base s
            INNER JOIN crmsa_base c ON s.ACCTNO = c.ACCTNO
            ORDER BY s.ACCTNO
        """).pl()
    
    conn.register("savg", savg_df)
    
    # 3. Load other datasets
    saacc_df = conn.execute(f"""
        SELECT * FROM read_csv('{input_path}/SIGNA/SMSACC.csv')
        ORDER BY ACCTNO
    """).pl()
    
    dpclos_df = conn.execute(f"""
        WITH dpclos_raw AS (
            SELECT * FROM read_csv('{input_path}/PRODEV/DPCLOS.csv')
        ),
        dpclos_sorted AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY ACCTNO ORDER BY CLOSEDT DESC) AS rn
            FROM dpclos_raw
        )
        SELECT ACCTNO, CLOSEDT, RCODE
        FROM dpclos_sorted
        WHERE rn = 1
        ORDER BY ACCTNO
    """).pl()
    
    conn.register("saacc", saacc_df)
    conn.register("dpclos", dpclos_df)
    
    # 4. Execute final merge
    final_df = conn.execute(f"""
        WITH ddwh AS (
            SELECT 
                c.*,
                s.* EXCLUDE (ACCTNO),
                sa.* EXCLUDE (ACCTNO)
            FROM cis c
            INNER JOIN savg s ON c.ACCTNO = s.ACCTNO
            LEFT JOIN saacc sa ON c.ACCTNO = sa.ACCTNO
        ),
        dates_transformed AS (
            SELECT 
                *,
                CASE 
                    WHEN OPENDT > 0 THEN 
                        strptime(
                            substr(lpad(cast(OPENDT as varchar), 11, '0'), 1, 8),
                            '%m%d%Y'
                        )
                END AS ODATE,
                CASE 
                    WHEN CLOSEDT > 0 THEN 
                        strptime(
                            substr(lpad(cast(CLOSEDT as varchar), 11, '0'), 1, 8),
                            '%m%d%Y'
                        )
                END AS CDATE,
                COALESCE(NULLIF(ESIGNATURE, ''), 'N') AS ESIGNATURE_FIXED
            FROM ddwh
        ),
        saclose AS (
            SELECT 
                * EXCLUDE (OPENDT, CLOSEDT, ESIGNATURE),
                ODATE AS OPENDT,
                CDATE AS CLOSEDT,
                ESIGNATURE_FIXED AS ESIGNATURE
            FROM dates_transformed
        ),
        mis_mtd AS (
            SELECT * FROM read_csv('{input_path}/MISMTD/SAVG{REPTMON}.csv')
        )
        
        SELECT 
            s.*,
            d.CLOSEDT AS DPCLOS_CLOSEDT,
            d.RCODE,
            m.* EXCLUDE (ACCTNO)
        FROM saclose s
        LEFT JOIN dpclos d ON s.ACCTNO = d.ACCTNO
        LEFT JOIN mis_mtd m ON s.ACCTNO = m.ACCTNO
        ORDER BY s.ACCTNO
    """).pl()
    
    conn.close()
    
    # Save result
    output_file = output_path / f"SACLOSE{REPTMON}{REPTYEAR}_optimized.parquet"
    final_df.write_parquet(output_file)
    
    print(f"Optimized processing complete!")
    print(f"Output saved to: {output_file}")
    
    return final_df

if __name__ == "__main__":
    # Option 1: Main hybrid version
    result = main()
    
    # Option 2: Polars-only version
    # result = alternative_polars_only_version()
    
    # Option 3: Optimized hybrid version (most efficient)
    # result = run_hybrid_optimized()
