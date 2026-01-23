import polars as pl
import duckdb
from pathlib import Path
import datetime
from dateutil.relativedelta import relativedelta

# Configure input file paths
input_path = Path("/path/to/input")  # Update with your actual path
output_path = Path("/path/to/output")  # Update with your actual path

# Load REPTDATE data (assuming CSV or similar format)
# You'll need to adjust the file format based on your actual data source
def load_reptdate():
    # Assuming DEP.REPTDATE is a CSV file
    reptdate_df = pl.read_csv(input_path / "DEP/REPTDATE.csv")
    
    # Calculate REPTYEAR, REPTMON, REPTMON1
    reptdate_df = reptdate_df.with_columns([
        pl.col("REPTDATE").str.strptime(pl.Date, format="%Y-%m-%d"),
        (pl.col("REPTDATE").str.strptime(pl.Date, format="%Y-%m-%d").dt.month()).alias("MM")
    ])
    
    # Calculate MM1 based on SAS logic
    reptdate_df = reptdate_df.with_columns([
        ((pl.col("MM") - 1) + 12 * ((pl.col("MM") - 1) == 0)).alias("MM1")
    ])
    
    # Get the first row values for macros
    first_row = reptdate_df.row(0)
    REPTYEAR = str(first_row[0].year)[-2:]  # Last 2 digits of year
    REPTMON = f"{first_row[1]:02d}"  # MM with leading zero
    REPTMON1 = f"{first_row[2]:02d}"  # MM1 with leading zero
    
    return REPTYEAR, REPTMON, REPTMON1

def process_cis_data():
    """Process CIS data similar to SAS CIS dataset creation"""
    # Assuming CIS.DEPOSIT is a CSV or Parquet file
    cis_df = pl.read_csv(input_path / "CIS/DEPOSIT.csv")
    
    # Filter where SECCUST = '901'
    cis_df = cis_df.filter(pl.col("SECCUST") == "901")
    
    # Extract date components and create DOBCIS
    cis_df = cis_df.with_columns([
        pl.col("BIRTHDAT").str.slice(0, 2).cast(pl.Int32).alias("BDD"),
        pl.col("BIRTHDAT").str.slice(2, 2).cast(pl.Int32).alias("BMM"),
        pl.col("BIRTHDAT").str.slice(4, 4).cast(pl.Int32).alias("BYY"),
        pl.col("CITIZEN").alias("COUNTRY")
    ]).with_columns([
        pl.date(pl.col("BYY"), pl.col("BMM"), pl.col("BDD")).alias("DOBCIS")
    ])
    
    # Keep required columns and remove duplicates
    cis_df = cis_df.select(["ACCTNO", "DOBCIS", "OCCUPAT", "COUNTRY", "RACE"])
    cis_df = cis_df.unique(subset=["ACCTNO"], keep="first")
    
    return cis_df

def monthly_processing(REPTMON, REPTMON1, NOWK=None):
    """Handle monthly processing logic"""
    
    # Load CURR data
    curr_df = pl.read_csv(input_path / f"MIS/CURRC{REPTMON}.csv")
    curr_df = curr_df.sort("ACCTNO")
    
    # Check conditions similar to SAS macro
    if NOWK != "4" and REPTMON == "01":
        # Initialize FEEYTD to 0
        curr_df = curr_df.with_columns(pl.lit(0).alias("FEEYTD"))
    else:
        # Load and merge CRMCA data
        crmca_df = pl.read_csv(input_path / f"MNICRM/CA{REPTMON1}.csv")
        crmca_df = crmca_df.unique(subset=["ACCTNO"], keep="first")
        crmca_df = crmca_df.drop(["COSTCTR", "FEEPD"])
        
        # Merge with CURR
        curr_df = curr_df.join(crmca_df, on="ACCTNO", how="left")
    
    return curr_df

def main():
    # Step 1: Get report date parameters
    REPTYEAR, REPTMON, REPTMON1 = load_reptdate()
    
    # Step 2: Process CIS data
    cis_df = process_cis_data()
    
    # Step 3: Load additional datasets
    curr_df = pl.read_csv(input_path / f"MIS/CURRC{REPTMON}.csv").sort("ACCTNO")
    saacc_df = pl.read_csv(input_path / "SIGNA/SMSACC.csv").sort("ACCTNO")
    
    # Step 4: Monthly processing
    # You need to define NOWK based on your logic
    NOWK = None  # Define this based on your requirements
    curr_df = monthly_processing(REPTMON, REPTMON1, NOWK)
    
    # Step 5: Merge datasets (DDWH creation)
    ddwh_df = cis_df.join(curr_df, on="ACCTNO", how="inner")
    ddwh_df = ddwh_df.join(saacc_df, on="ACCTNO", how="left")
    
    # Step 6: Date transformations
    ddwh_df = ddwh_df.with_columns([
        pl.when(pl.col("OPENDT") > 0)
        .then(
            pl.col("OPENDT").cast(pl.Utf8).str.zfill(11).str.slice(0, 8)
            .str.strptime(pl.Date, format="%m%d%Y")
        )
        .alias("ODATE"),
        pl.when(pl.col("CLOSEDT") > 0)
        .then(
            pl.col("CLOSEDT").cast(pl.Utf8).str.zfill(11).str.slice(0, 8)
            .str.strptime(pl.Date, format="%m%d%Y")
        )
        .alias("CDATE")
    ]).with_columns([
        pl.when(pl.col("ESIGNATURE") == "")
        .then(pl.lit("N"))
        .otherwise(pl.col("ESIGNATURE"))
        .alias("ESIGNATURE")
    ]).drop(["OPENDT", "CLOSEDT"])
    
    # Step 7: Create CACLOSE dataset
    caclose_df = ddwh_df.with_columns([
        pl.col("ODATE").alias("OPENDT"),
        pl.col("CDATE").alias("CLOSEDT"),
        pl.col("RISKCODE").cast(pl.Utf8).str.slice(0, 1).alias("RISKRATE")
    ]).drop(["ODATE", "CDATE"])
    
    # Step 8: Process DPCLOS data
    dpclos_df = pl.read_csv(input_path / "PRODEV/DPCLOS.csv")
    
    # Keep latest by descending CLOSEDT and remove duplicates
    dpclos_df = (
        dpclos_df.sort("CLOSEDT", descending=True)
        .unique(subset=["ACCTNO"], keep="first")
        .select(["ACCTNO", "CLOSEDT", "RCODE"])
        .sort("ACCTNO")
    )
    
    # Step 9: Load ACCUM data
    accum_df = pl.read_csv(input_path / "DRCRCA/ACCUM.csv").sort("ACCTNO")
    
    # Step 10: Load CAVG data
    cavg_df = pl.read_csv(input_path / f"MISMTD/CAVG{REPTMON}.csv")
    cavg_df = cavg_df.select(["ACCTNO", "MTDAVBAL_MIS"])
    
    # Step 11: Final merge and output
    final_df = (
        caclose_df
        .join(dpclos_df, on="ACCTNO", how="left")
        .join(accum_df, on="ACCTNO", how="left")
        .join(cavg_df, on="ACCTNO", how="left")
    )
    
    # Save to Parquet
    output_file = output_path / f"CACLOSE{REPTMON}{REPTYEAR}.parquet"
    final_df.write_parquet(output_file)
    print(f"Output saved to: {output_file}")
    
    return final_df

if __name__ == "__main__":
    result_df = main()
    print("Processing completed successfully!")
