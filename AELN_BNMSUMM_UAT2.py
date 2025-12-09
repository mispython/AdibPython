import polars as pl
import pyreadstat
from datetime import datetime
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Global delimiter for output
DELIMITER = "\x05"

def read_sas7bdat(file_path: str) -> pl.DataFrame:
    """Read SAS7BDAT file using pyreadstat"""
    df, meta = pyreadstat.read_sas7bdat(file_path)
    return pl.from_pandas(df)

def process_reptdate():
    """Process REPTDATE dataset and extract report parameters"""
    reptdate_df = read_sas7bdat("DEPOSIT/REPTDATE.sas7bdat")
    
    # Get first date (assuming single record)
    reptdate = reptdate_df["REPTDATE"][0]
    
    # Determine week number based on day
    day = reptdate.day
    if day == 8: nowk = '1'
    elif day == 15: nowk = '2'
    elif day == 22: nowk = '3'
    else: nowk = '4'
    
    # Format year and month
    reptyear = str(reptdate.year)[-2:]
    reptmon = f"{reptdate.month:02d}"
    
    return nowk, reptyear, reptmon

def split_inward_outward():
    """Main processing: split data into inward and outward"""
    
    # Get report parameters
    nowk, reptyear, reptmon = process_reptdate()
    
    # Read transaction data
    dataset_name = f"RMT/REMTRAN{reptmon}{nowk}{reptyear}.sas7bdat"
    df = read_sas7bdat(dataset_name)
    
    # Filter and select columns (SAS WHERE and KEEP)
    filtered = df.filter(
        (pl.col("REMTYPE") == "F") & 
        (pl.col("STATUS").is_in(["TI", "TO", "BR"]))
    ).select([
        "BRANCHABB", "STATUS", "SERIAL", "LASTTRAN",
        "ANAD1", "ANAD2", "ANAD3", "ANAD4",
        "BNAD1", "BNAD2", "BNAD3", "BNAD4",
        "AMOUNT", "FORAMT", "PAYREF", "ACCTNO",
        "CURRENCY", "PAYMODE", "USERID", "RESIDENT",
        "APPLNATIONAL", "BMRSTATUS", "COUNTRY", "ADMIN"
    ])
    
    # Split into inward/outward (SAS IF/ELSE OUTPUT)
    inward = filtered.filter(pl.col("STATUS") == "TI")
    outward = filtered.filter(pl.col("STATUS").is_in(["TO", "BR"]))
    
    return inward, outward

def write_delimited_report(df: pl.DataFrame, output_path: str, is_inward: bool = True):
    """Write delimited report file (mimics SAS FILE/PUT)"""
    
    # Define headers based on report type
    if is_inward:
        headers = [
            "TRANSACTION DATE", "BRANCH NAME", "REFERENCE NO",
            "BENEFICIARY NAME 1", "BENEFICIARY NAME 2", "BENEFICIARY NAME 3", "BENEFICIARY NAME 4",
            "ORDERING CUSTOMER 1", "ORDERING CUSTOMER 2", "ORDERING CUSTOMER 3", "ORDERING CUSTOMER 4",
            "AMOUNT (FCY)", "AMOUNT (MYR)", "ACCOUNT NO", "CURRENCY", "PAYMENT MODE",
            "STAFF ID", "RESIDENCY", "NATIONALITY", "BMR STATUS", "COUNTRY", "CBOP CODE"
        ]
    else:
        headers = [
            "TRANSACTION DATE", "BRANCH NAME", "REFERENCE NO",
            "ORDERING CUSTOMER 1", "ORDERING CUSTOMER 2", "ORDERING CUSTOMER 3", "ORDERING CUSTOMER 4",
            "ORDERING CUSTOMER ACCOUNT NO", "BENEFICIARY NAME 1", "BENEFICIARY NAME 2",
            "BENEFICIARY NAME 3", "BENEFICIARY NAME 4", "AMOUNT (FCY)", "AMOUNT (MYR)",
            "ACCOUNT NO", "CURRENCY", "PAYMENT MODE", "STAFF ID", "RESIDENCY",
            "NATIONALITY", "BMR STATUS", "COUNTRY", "CBOP CODE"
        ]
    
    with open(output_path, "w", encoding="utf-8") as f:
        # Write header (SAS IF _N_ = 1)
        f.write(DELIMITER.join(headers) + "\n")
        
        # Write data rows
        for row in df.iter_rows(named=True):
            if is_inward:
                data = [
                    str(row.get("LASTTRAN", "")),
                    str(row.get("BRANCHABB", "")),
                    str(row.get("SERIAL", "")),
                    str(row.get("BNAD1", "")),
                    str(row.get("BNAD2", "")),
                    str(row.get("BNAD3", "")),
                    str(row.get("BNAD4", "")),
                    str(row.get("ANAD1", "")),
                    str(row.get("ANAD2", "")),
                    str(row.get("ANAD3", "")),
                    str(row.get("ANAD4", "")),
                    str(row.get("FORAMT", "")),
                    str(row.get("AMOUNT", "")),
                    str(row.get("ACCTNO", "")),
                    str(row.get("CURRENCY", "")),
                    str(row.get("PAYMODE", "")),
                    str(row.get("USERID", "")),
                    str(row.get("RESIDENT", "")),
                    str(row.get("APPLNATIONAL", "")),
                    str(row.get("BMRSTATUS", "")),
                    str(row.get("COUNTRY", "")),
                    str(row.get("ADMIN", ""))
                ]
            else:
                data = [
                    str(row.get("LASTTRAN", "")),
                    str(row.get("BRANCHABB", "")),
                    str(row.get("SERIAL", "")),
                    str(row.get("ANAD1", "")),
                    str(row.get("ANAD2", "")),
                    str(row.get("ANAD3", "")),
                    str(row.get("ANAD4", "")),
                    str(row.get("PAYREF", "")),
                    str(row.get("BNAD1", "")),
                    str(row.get("BNAD2", "")),
                    str(row.get("BNAD3", "")),
                    str(row.get("BNAD4", "")),
                    str(row.get("FORAMT", "")),
                    str(row.get("AMOUNT", "")),
                    str(row.get("ACCTNO", "")),
                    str(row.get("CURRENCY", "")),
                    str(row.get("PAYMODE", "")),
                    str(row.get("USERID", "")),
                    str(row.get("RESIDENT", "")),
                    str(row.get("APPLNATIONAL", "")),
                    str(row.get("BMRSTATUS", "")),
                    str(row.get("COUNTRY", "")),
                    str(row.get("ADMIN", ""))
                ]
            
            f.write(DELIMITER.join(data) + "\n")

def main():
    """Main execution following SAS logic"""
    
    print("Processing SAS remittance reports...")
    
    # Split data (SAS DATA INWARD OUTWARD)
    inward_df, outward_df = split_inward_outward()
    
    print(f"Inward records: {len(inward_df)}")
    print(f"Outward records: {len(outward_df)}")
    
    # Write delimited reports (SAS DATA _NULL_)
    write_delimited_report(inward_df, "INWREPT.txt", is_inward=True)
    write_delimited_report(outward_df, "OUTWREPT.txt", is_inward=False)
    
    # Write to Parquet files (additional output)
    inward_df.write_parquet("inward.parquet")
    outward_df.write_parquet("outward.parquet")
    
    print("Reports generated:")
    print("  - INWREPT.txt (delimited)")
    print("  - OUTWREPT.txt (delimited)")
    print("  - inward.parquet")
    print("  - outward.parquet")

# Single-file version (even shorter)
def concise_version():
    """Most concise version that still follows SAS logic"""
    
    # Read control date
    rept_df = pyreadstat.read_sas7bdat("DEPOSIT/REPTDATE.sas7bdat")[0]
    reptdate = rept_df["REPTDATE"][0]
    
    # Set parameters like SAS CALL SYMPUT
    day = reptdate.day
    nowk = '1' if day == 8 else '2' if day == 15 else '3' if day == 22 else '4'
    reptyear, reptmon = str(reptdate.year)[-2:], f"{reptdate.month:02d}"
    
    # Read and filter transaction data
    df = pl.from_pandas(
        pyreadstat.read_sas7bdat(f"RMT/REMTRAN{reptmon}{nowk}{reptyear}.sas7bdat")[0]
    )
    
    # Apply SAS WHERE and KEEP
    df = df.filter(
        (pl.col("REMTYPE") == "F") & 
        (pl.col("STATUS").is_in(["TI", "TO", "BR"]))
    ).select([
        "BRANCHABB", "STATUS", "SERIAL", "LASTTRAN",
        "ANAD1", "ANAD2", "ANAD3", "ANAD4",
        "BNAD1", "BNAD2", "BNAD3", "BNAD4",
        "AMOUNT", "FORAMT", "PAYREF", "ACCTNO",
        "CURRENCY", "PAYMODE", "USERID", "RESIDENT",
        "APPLNATIONAL", "BMRSTATUS", "COUNTRY", "ADMIN"
    ])
    
    # Split like SAS IF/ELSE OUTPUT
    inward = df.filter(pl.col("STATUS") == "TI")
    outward = df.filter(pl.col("STATUS") != "TI")
    
    # Save outputs
    inward.write_parquet("inward.parquet")
    outward.write_parquet("outward.parquet")
    
    return inward, outward

if __name__ == "__main__":
    # Install required: pip install polars pyreadstat pandas
    main()
    
    # Or use the concise version:
    # concise_version()
