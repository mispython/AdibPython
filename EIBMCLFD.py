import polars as pl
import duckdb
from pathlib import Path
from typing import Union

# Configure input file paths
input_path = Path("/path/to/input")  # Update with your actual path
output_path = Path("/path/to/output")  # Update with your actual path

def load_reptdate() -> tuple[str, str]:
    """Load REPTDATE data and extract year/month using Polars"""
    # Read REPTDATE - adjust format based on your file type
    reptdate_df = pl.read_csv(input_path / "DEP/REPTDATE.csv")
    
    # Parse date and extract month - adjust format as needed
    reptdate_df = reptdate_df.with_columns([
        pl.col("REPTDATE").str.strptime(pl.Date, format="%Y-%m-%d"),
        pl.col("REPTDATE").str.strptime(pl.Date, format="%Y-%m-%d").dt.month().alias("MM")
    ])
    
    # Get values from first row
    first_row = reptdate_df.row(0)
    REPTYEAR = str(first_row[0].year)[-2:]  # Last 2 digits of year
    REPTMON = f"{first_row[1]:02d}"  # Month with leading zero
    
    return REPTYEAR, REPTMON

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

def merge_with_duckdb(
    cis_df: pl.DataFrame,
    reptmon: str,
    reptyear: str
) -> pl.DataFrame:
    """
    Use DuckDB for complex SAS-style merges
    This mirrors the SAS DATA step merge logic
    """
    # Create DuckDB connection
    conn = duckdb.connect()
    
    # Register Polars DataFrame with DuckDB
    conn.register("cis", cis_df)
    
    # Load FD data directly using DuckDB's file reading
    # This avoids loading all data into memory
    conn.execute(f"""
        CREATE OR REPLACE TEMP VIEW fd AS
        SELECT * 
        FROM read_csv('{input_path}/MIS/FDC{reptmon}.csv')
        ORDER BY ACCTNO
    """)
    
    # Load CRMFD data and process (similar to SAS PROC SORT NODUPKEY)
    conn.execute(f"""
        CREATE OR REPLACE TEMP VIEW crmfd_clean AS
        SELECT DISTINCT ACCTNO, * EXCLUDE (ACCTNO, COSTCTR, FEEPD)
        FROM read_csv('{input_path}/MNICRM/FD{reptmon}.csv')
        ORDER BY ACCTNO
    """)
    
    # Merge FD with CRMFD (similar to SAS DATA step merge)
    conn.execute("""
        CREATE OR REPLACE TEMP VIEW fd_merged AS
        SELECT 
            fd.*,
            crmfd.* EXCLUDE (ACCTNO)
        FROM fd
        INNER JOIN crmfd_clean crmfd 
            ON fd.ACCTNO = crmfd.ACCTNO
    """)
    
    # Load other datasets
    conn.execute(f"""
        CREATE OR REPLACE TEMP VIEW saacc AS
        SELECT * 
        FROM read_csv('{input_path}/SIGNA/SMSACC.csv')
        ORDER BY ACCTNO
    """)
    
    conn.execute(f"""
        CREATE OR REPLACE TEMP VIEW favg AS
        SELECT ACCTNO, MTDAVBAL_MIS
        FROM read_csv('{input_path}/MISMTD/FAVG{reptmon}.csv')
    """)
    
    # Execute the main merge (mirrors SAS DATA step merge)
    # This is where DuckDB shines for complex joins
    result = conn.execute("""
        WITH merged_data AS (
            SELECT 
                cis.ACCTNO,
                cis.DOBCIS,
                cis.OCCUPAT,
                cis.COUNTRY,
                cis.RACE,
                fd.* EXCLUDE (ACCTNO),
                saacc.* EXCLUDE (ACCTNO),
                favg.MTDAVBAL_MIS
            FROM cis
            INNER JOIN fd_merged fd ON cis.ACCTNO = fd.ACCTNO
            LEFT JOIN saacc ON cis.ACCTNO = saacc.ACCTNO
            LEFT JOIN favg ON cis.ACCTNO = favg.ACCTNO
        )
        SELECT *
        FROM merged_data
        ORDER BY ACCTNO
    """).pl()  # Convert back to Polars DataFrame
    
    conn.close()
    return result

def main():
    """Main processing function using hybrid approach"""
    print("Starting EIBMCLFD processing...")
    
    # Step 1: Get report date parameters (Polars)
    REPTYEAR, REPTMON = load_reptdate()
    print(f"Processing for month: {REPTMON}, year: {REPTYEAR}")
    
    # Step 2: Process CIS data (Polars - excellent for transformations)
    print("Processing CIS data...")
    cis_df = process_cis_with_polars()
    print(f"CIS data shape: {cis_df.shape}")
    
    # Step 3: Complex merges (DuckDB - excellent for SQL-style joins)
    print("Merging datasets...")
    merged_df = merge_with_duckdb(cis_df, REPTMON, REPTYEAR)
    print(f"Merged data shape: {merged_df.shape}")
    
    # Step 4: Date transformations (Polars - excellent for column operations)
    print("Transforming dates...")
    transformed_df = transform_dates_with_polars(merged_df)
    
    # Step 5: Final adjustments (Polars)
    print("Creating final dataset...")
    final_df = transformed_df.with_columns([
        pl.col("ODATE").alias("OPENDT"),
        pl.col("CDATE").alias("CLOSEDT")
    ]).drop(["ODATE", "CDATE"])
    
    # Step 6: Save to Parquet (Polars)
    output_file = output_path / f"FDCLOSE{REPTMON}{REPTYEAR}.parquet"
    final_df.write_parquet(output_file)
    
    # Also save metadata
    metadata_file = output_path / f"FDCLOSE{REPTMON}{REPTYEAR}_metadata.txt"
    with open(metadata_file, 'w') as f:
        f.write(f"Records: {len(final_df)}\n")
        f.write(f"Columns: {final_df.columns}\n")
        f.write(f"Generated: {datetime.datetime.now()}\n")
    
    print(f"✅ Processing complete!")
    print(f"   Output saved to: {output_file}")
    print(f"   Records processed: {len(final_df):,}")
    print(f"   File size: {output_file.stat().st_size / (1024*1024):.2f} MB")
    
    return final_df

def alternative_simple_version():
    """
    Simpler version using mostly Polars with DuckDB only for joins
    if you prefer less DuckDB usage
    """
    REPTYEAR, REPTMON = load_reptdate()
    
    # Process CIS with Polars
    cis_df = process_cis_with_polars()
    
    # Load other datasets with Polars
    fd_df = pl.read_csv(input_path / f"MIS/FDC{REPTMON}.csv").sort("ACCTNO")
    crmfd_df = (
        pl.read_csv(input_path / f"MNICRM/FD{REPTMON}.csv")
        .unique(subset=["ACCTNO"], keep="first")
        .drop(["COSTCTR", "FEEPD"])
    )
    saacc_df = pl.read_csv(input_path / "SIGNA/SMSACC.csv").sort("ACCTNO")
    favg_df = pl.read_csv(input_path / f"MISMTD/FAVG{REPTMON}.csv")
    favg_df = favg_df.select(["ACCTNO", "MTDAVBAL_MIS"])
    
    # Use DuckDB only for the complex merge
    conn = duckdb.connect()
    conn.register("cis", cis_df)
    conn.register("fd", fd_df)
    conn.register("crmfd", crmfd_df)
    conn.register("saacc", saacc_df)
    conn.register("favg", favg_df)
    
    # SAS-style merge logic in SQL
    result = conn.execute("""
        WITH fd_merged AS (
            SELECT fd.*, crmfd.* EXCLUDE (ACCTNO)
            FROM fd
            INNER JOIN crmfd ON fd.ACCTNO = crmfd.ACCTNO
        )
        SELECT 
            cis.ACCTNO,
            cis.DOBCIS,
            cis.OCCUPAT,
            cis.COUNTRY,
            cis.RACE,
            fd_merged.* EXCLUDE (ACCTNO),
            saacc.* EXCLUDE (ACCTNO),
            favg.MTDAVBAL_MIS
        FROM cis
        INNER JOIN fd_merged ON cis.ACCTNO = fd_merged.ACCTNO
        LEFT JOIN saacc ON cis.ACCTNO = saacc.ACCTNO
        LEFT JOIN favg ON cis.ACCTNO = favg.ACCTNO
    """).pl()
    
    conn.close()
    
    # Continue with Polars for transformations
    result = transform_dates_with_polars(result)
    result = result.with_columns([
        pl.col("ODATE").alias("OPENDT"),
        pl.col("CDATE").alias("CLOSEDT")
    ]).drop(["ODATE", "CDATE"])
    
    # Save result
    output_file = output_path / f"FDCLOSE{REPTMON}{REPTYEAR}_simple.parquet"
    result.write_parquet(output_file)
    
    return result

if __name__ == "__main__":
    # Run main hybrid version
    result = main()
    
    # Optionally run simple version
    # result = alternative_simple_version()
    
    print("\nSample output:")
    print(result.select(["ACCTNO", "DOBCIS", "OPENDT", "MTDAVBAL_MIS"]).head())
