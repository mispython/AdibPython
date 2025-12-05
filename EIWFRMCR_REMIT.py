import polars as pl
from pathlib import Path
import datetime
import pandas as pd
from typing import Tuple

# Define the delimiter (ASCII ENQ character 05)
DELIMITER = "\x05"  # This is the 1:1 translation of SAS's DLM = '05'X

# ============================================
# SAS: DATA REPTDATE; SET DEPOSIT.REPTDATE;
# ============================================
def process_reptdate(input_path: str = "DEPOSIT/REPTDATE.csv") -> Tuple[str, str, str]:
    """
    Exact translation of:
    DATA REPTDATE;
       SET DEPOSIT.REPTDATE;
       SELECT(DAY(REPTDATE));
          WHEN(8)   CALL SYMPUT('NOWK', PUT('1', $1.));
          WHEN(15)  CALL SYMPUT('NOWK', PUT('2', $1.));
          WHEN(22)  CALL SYMPUT('NOWK', PUT('3', $1.));
          OTHERWISE CALL SYMPUT('NOWK', PUT('4', $1.));
       END;
       CALL SYMPUT('REPTYEAR',PUT(REPTDATE,YEAR2.));
       CALL SYMPUT('REPTMON',PUT(MONTH(REPTDATE),Z2.));
    RUN;
    """
    # Read the data - assuming CSV format
    reptdate_df = pl.read_csv(input_path)
    
    # Assuming REPTDATE is a date column - adjust parsing as needed
    # Convert to datetime if it's not already
    if reptdate_df.schema["REPTDATE"] != pl.Date:
        reptdate_df = reptdate_df.with_columns(
            pl.col("REPTDATE").str.strptime(pl.Date, format="%Y-%m-%d")
        )
    
    # Get the first row (SAS would process all, but we take first like your example)
    row = reptdate_df.row(0, named=True)
    reptdate = row["REPTDATE"]
    
    # SAS SELECT(DAY(REPTDATE)) logic
    day = reptdate.day
    if day == 8:
        nowk = '1'
    elif day == 15:
        nowk = '2'
    elif day == 22:
        nowk = '3'
    else:
        nowk = '4'
    
    # SAS YEAR2. format (last 2 digits of year)
    reptyear = str(reptdate.year)[-2:]
    
    # SAS Z2. format (2 digits with leading zero)
    reptmon = f"{reptdate.month:02d}"
    
    print(f"SAS Macro Variables Set:")
    print(f"  NOWK: {nowk}")
    print(f"  REPTYEAR: {reptyear}")
    print(f"  REPTMON: {reptmon}")
    
    return nowk, reptyear, reptmon

# ============================================
# SAS: DATA INWARD OUTWARD; SET RMT.REMTRAN...;
# ============================================
def create_inward_outward(nowk: str, reptyear: str, reptmon: str, 
                         input_dir: str = "RMT") -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Exact translation of:
    DATA INWARD OUTWARD;
       SET RMT.REMTRAN&REPTMON&NOWK&REPTYEAR;
       WHERE REMTYPE = 'F' AND STATUS IN ('TI','TO','BR');
    
       IF STATUS = 'TI' THEN OUTPUT INWARD;
       ELSE OUTPUT OUTWARD;
    
       KEEP BRANCHABB STATUS SERIAL LASTTRAN ANAD1 ANAD2 ANAD3 ANAD4
            BNAD1 BNAD2 BNAD3 BNAD4 AMOUNT FORAMT PAYREF
            ACCTNO CURRENCY PAYMODE USERID RESIDENT APPLNATIONAL
            BMRSTATUS COUNTRY ADMIN;
    RUN;
    """
    # Construct dataset name like SAS macro variables
    dataset_name = f"REMTRAN{reptmon}{nowk}{reptyear}"
    input_path = Path(input_dir) / f"{dataset_name}.csv"
    
    # Read the data
    df = pl.read_csv(input_path)
    
    # SAS WHERE clause
    filtered = df.filter(
        (pl.col("REMTYPE") == "F") & 
        (pl.col("STATUS").is_in(["TI", "TO", "BR"]))
    )
    
    # SAS KEEP statement (select specific columns)
    columns_to_keep = [
        "BRANCHABB", "STATUS", "SERIAL", "LASTTRAN",
        "ANAD1", "ANAD2", "ANAD3", "ANAD4",
        "BNAD1", "BNAD2", "BNAD3", "BNAD4",
        "AMOUNT", "FORAMT", "PAYREF",
        "ACCTNO", "CURRENCY", "PAYMODE", "USERID", "RESIDENT",
        "APPLNATIONAL", "BMRSTATUS", "COUNTRY", "ADMIN"
    ]
    
    # Ensure all columns exist
    existing_columns = [col for col in columns_to_keep if col in filtered.columns]
    filtered = filtered.select(existing_columns)
    
    # SAS IF STATUS = 'TI' THEN OUTPUT INWARD; ELSE OUTPUT OUTWARD;
    inward = filtered.filter(pl.col("STATUS") == "TI")
    outward = filtered.filter(pl.col("STATUS") != "TI")
    
    print(f"Records processed:")
    print(f"  Inward (TI): {len(inward)} records")
    print(f"  Outward (TO/BR): {len(outward)} records")
    
    return inward, outward

# ============================================
# SAS: DATA _NULL_; SET INWARD; FILE INWREPT;
# ============================================
def write_inward_report(inward_df: pl.DataFrame, output_file: str = "INWREPT.txt"):
    """
    Exact translation of:
    DATA _NULL_;
       SET INWARD;
       FILE INWREPT;
       DLM = '05'X;
       IF _N_ = 1 THEN DO;
          PUT 'TRANSACTION DATE '    +(-1)DLM+(-1)
              'BRANCH NAME '         +(-1)DLM+(-1)
              ... etc ...
              'CBOP CODE '           +(-1)DLM+(-1);
       END;
    
       PUT LASTTRAN  +(-1)DLM+(-1)
           BRANCHABB +(-1)DLM+(-1)
           ... etc ...
           ADMIN     +(-1)DLM+(-1);
    RUN;
    """
    
    with open(output_file, "w", encoding="utf-8") as f:
        # Write header (SAS: IF _N_ = 1 THEN DO;)
        header = [
            "TRANSACTION DATE",
            "BRANCH NAME",
            "REFERENCE NO",
            "BENEFICIARY NAME 1",
            "BENEFICIARY NAME 2",
            "BENEFICIARY NAME 3",
            "BENEFICIARY NAME 4",
            "ORDERING CUSTOMER 1",
            "ORDERING CUSTOMER 2",
            "ORDERING CUSTOMER 3",
            "ORDERING CUSTOMER 4",
            "AMOUNT (FCY)",
            "AMOUNT (MYR)",
            "ACCOUNT NO",
            "CURRENCY",
            "PAYMENT MODE",
            "STAFF ID",
            "RESIDENCY",
            "NATIONALITY",
            "BMR STATUS",
            "COUNTRY",
            "CBOP CODE"
        ]
        
        # SAS: +(-1)DLM+(-1) adds delimiter between items
        f.write(DELIMITER.join(header) + "\n")
        
        # Write data rows
        for row in inward_df.iter_rows(named=True):
            # SAS PUT statements with delimiter
            data_row = [
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
            
            f.write(DELIMITER.join(data_row) + "\n")
    
    print(f"Inward report written to: {output_file}")

# ============================================
# SAS: DATA _NULL_; SET OUTWARD; FILE OUTWREPT;
# ============================================
def write_outward_report(outward_df: pl.DataFrame, output_file: str = "OUTWREPT.txt"):
    """
    Exact translation of second DATA _NULL_ step
    """
    
    with open(output_file, "w", encoding="utf-8") as f:
        # Write header
        header = [
            "TRANSACTION DATE",
            "BRANCH NAME",
            "REFERENCE NO",
            "ORDERING CUSTOMER 1",
            "ORDERING CUSTOMER 2",
            "ORDERING CUSTOMER 3",
            "ORDERING CUSTOMER 4",
            "ORDERING CUSTOMER ACCOUNT NO",
            "BENEFICIARY NAME 1",
            "BENEFICIARY NAME 2",
            "BENEFICIARY NAME 3",
            "BENEFICIARY NAME 4",
            "AMOUNT (FCY)",
            "AMOUNT (MYR)",
            "ACCOUNT NO",
            "CURRENCY",
            "PAYMENT MODE",
            "STAFF ID",
            "RESIDENCY",
            "NATIONALITY",
            "BMR STATUS",
            "COUNTRY",
            "CBOP CODE"
        ]
        
        f.write(DELIMITER.join(header) + "\n")
        
        # Write data rows
        for row in outward_df.iter_rows(named=True):
            data_row = [
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
            
            f.write(DELIMITER.join(data_row) + "\n")
    
    print(f"Outward report written to: {output_file}")

# ============================================
# Main execution - mirrors SAS program flow
# ============================================
def main():
    """
    Executes the complete SAS program flow:
    1. Process REPTDATE and set macro variables
    2. Create INWARD and OUTWARD datasets
    3. Write INWARD report
    4. Write OUTWARD report
    """
    print("=" * 60)
    print("SAS to Python 1:1 Conversion")
    print("=" * 60)
    
    # Step 1: Process REPTDATE (sets NOWK, REPTYEAR, REPTMON)
    print("\n[Step 1] Processing REPTDATE dataset...")
    nowk, reptyear, reptmon = process_reptdate()
    
    # Step 2: Create INWARD and OUTWARD datasets
    print("\n[Step 2] Creating INWARD and OUTWARD datasets...")
    inward_df, outward_df = create_inward_outward(nowk, reptyear, reptmon)
    
    # Step 3: Write INWARD report
    print("\n[Step 3] Writing INWARD report...")
    write_inward_report(inward_df)
    
    # Step 4: Write OUTWARD report
    print("\n[Step 4] Writing OUTWARD report...")
    write_outward_report(outward_df)
    
    print("\n" + "=" * 60)
    print("SAS Program Successfully Converted to Python")
    print("=" * 60)

# ============================================
# Alternative: Complete procedural version (closer to SAS)
# ============================================
def sas_style_procedural():
    """
    Even closer to SAS procedural style
    """
    # DATA REPTDATE;
    reptdate_data = pl.read_csv("DEPOSIT/REPTDATE.csv")
    
    # Assuming single row, take first
    reptdate = reptdate_data["REPTDATE"][0]
    
    # SELECT(DAY(REPTDATE));
    day = reptdate.day
    if day == 8:
        NOWK = '1'
    elif day == 15:
        NOWK = '2'
    elif day == 22:
        NOWK = '3'
    else:
        NOWK = '4'
    
    # YEAR2. format
    REPTYEAR = str(reptdate.year)[-2:]
    
    # Z2. format  
    REPTMON = f"{reptdate.month:02d}"
    
    # DATA INWARD OUTWARD;
    dataset_name = f"REMTRAN{REPTMON}{NOWK}{REPTYEAR}"
    all_data = pl.read_csv(f"RMT/{dataset_name}.csv")
    
    # WHERE clause
    filtered_data = all_data.filter(
        (pl.col("REMTYPE") == "F") & 
        (pl.col("STATUS").is_in(["TI", "TO", "BR"]))
    )
    
    # Keep specific columns
    keep_cols = [
        "BRANCHABB", "STATUS", "SERIAL", "LASTTRAN",
        "ANAD1", "ANAD2", "ANAD3", "ANAD4",
        "BNAD1", "BNAD2", "BNAD3", "BNAD4",
        "AMOUNT", "FORAMT", "PAYREF",
        "ACCTNO", "CURRENCY", "PAYMODE", "USERID", "RESIDENT",
        "APPLNATIONAL", "BMRSTATUS", "COUNTRY", "ADMIN"
    ]
    
    filtered_data = filtered_data.select(
        [col for col in keep_cols if col in filtered_data.columns]
    )
    
    # IF STATUS = 'TI' THEN OUTPUT INWARD; ELSE OUTPUT OUTWARD;
    INWARD = filtered_data.filter(pl.col("STATUS") == "TI")
    OUTWARD = filtered_data.filter(pl.col("STATUS") != "TI")
    
    # DATA _NULL_; SET INWARD; FILE INWREPT;
    with open("INWREPT.txt", "w", encoding="utf-8") as f:
        # Header
        header = DELIMITER.join([
            "TRANSACTION DATE", "BRANCH NAME", "REFERENCE NO",
            "BENEFICIARY NAME 1", "BENEFICIARY NAME 2", "BENEFICIARY NAME 3", "BENEFICIARY NAME 4",
            "ORDERING CUSTOMER 1", "ORDERING CUSTOMER 2", "ORDERING CUSTOMER 3", "ORDERING CUSTOMER 4",
            "AMOUNT (FCY)", "AMOUNT (MYR)", "ACCOUNT NO", "CURRENCY", "PAYMENT MODE",
            "STAFF ID", "RESIDENCY", "NATIONALITY", "BMR STATUS", "COUNTRY", "CBOP CODE"
        ]) + "\n"
        f.write(header)
        
        # Data
        for row in INWARD.iter_rows(named=True):
            line = DELIMITER.join([
                str(row.get("LASTTRAN", "")), str(row.get("BRANCHABB", "")),
                str(row.get("SERIAL", "")), str(row.get("BNAD1", "")),
                str(row.get("BNAD2", "")), str(row.get("BNAD3", "")),
                str(row.get("BNAD4", "")), str(row.get("ANAD1", "")),
                str(row.get("ANAD2", "")), str(row.get("ANAD3", "")),
                str(row.get("ANAD4", "")), str(row.get("FORAMT", "")),
                str(row.get("AMOUNT", "")), str(row.get("ACCTNO", "")),
                str(row.get("CURRENCY", "")), str(row.get("PAYMODE", "")),
                str(row.get("USERID", "")), str(row.get("RESIDENT", "")),
                str(row.get("APPLNATIONAL", "")), str(row.get("BMRSTATUS", "")),
                str(row.get("COUNTRY", "")), str(row.get("ADMIN", ""))
            ]) + "\n"
            f.write(line)
    
    # DATA _NULL_; SET OUTWARD; FILE OUTWREPT;
    with open("OUTWREPT.txt", "w", encoding="utf-8") as f:
        # Header
        header = DELIMITER.join([
            "TRANSACTION DATE", "BRANCH NAME", "REFERENCE NO",
            "ORDERING CUSTOMER 1", "ORDERING CUSTOMER 2", "ORDERING CUSTOMER 3", "ORDERING CUSTOMER 4",
            "ORDERING CUSTOMER ACCOUNT NO", "BENEFICIARY NAME 1", "BENEFICIARY NAME 2",
            "BENEFICIARY NAME 3", "BENEFICIARY NAME 4", "AMOUNT (FCY)", "AMOUNT (MYR)",
            "ACCOUNT NO", "CURRENCY", "PAYMENT MODE", "STAFF ID", "RESIDENCY",
            "NATIONALITY", "BMR STATUS", "COUNTRY", "CBOP CODE"
        ]) + "\n"
        f.write(header)
        
        # Data
        for row in OUTWARD.iter_rows(named=True):
            line = DELIMITER.join([
                str(row.get("LASTTRAN", "")), str(row.get("BRANCHABB", "")),
                str(row.get("SERIAL", "")), str(row.get("ANAD1", "")),
                str(row.get("ANAD2", "")), str(row.get("ANAD3", "")),
                str(row.get("ANAD4", "")), str(row.get("PAYREF", "")),
                str(row.get("BNAD1", "")), str(row.get("BNAD2", "")),
                str(row.get("BNAD3", "")), str(row.get("BNAD4", "")),
                str(row.get("FORAMT", "")), str(row.get("AMOUNT", "")),
                str(row.get("ACCTNO", "")), str(row.get("CURRENCY", "")),
                str(row.get("PAYMODE", "")), str(row.get("USERID", "")),
                str(row.get("RESIDENT", "")), str(row.get("APPLNATIONAL", "")),
                str(row.get("BMRSTATUS", "")), str(row.get("COUNTRY", "")),
                str(row.get("ADMIN", ""))
            ]) + "\n"
            f.write(line)

# ============================================
# Test function to verify the delimiter
# ============================================
def test_delimiter():
    """Test that the delimiter is correctly defined"""
    print("Testing delimiter...")
    print(f"Delimiter character: {repr(DELIMITER)}")
    print(f"Delimiter hex code: {DELIMITER.encode('utf-8').hex()}")
    print(f"Is this ASCII ENQ (05)?: {DELIMITER.encode('utf-8').hex() == '05'}")
    
    # Create a simple test file
    test_data = ["Field1", "Field2", "Field3"]
    test_line = DELIMITER.join(test_data)
    
    print(f"\nTest line: {repr(test_line)}")
    print(f"Expected (hex): {'05'.join([f.encode('utf-8').hex() for f in test_data])}")
    print(f"Actual (hex): {test_line.encode('utf-8').hex()}")

# ============================================
# Run the complete SAS program
# ============================================
if __name__ == "__main__":
    # First, test the delimiter
    test_delimiter()
    
    print("\n" + "=" * 60 + "\n")
    
    # Execute the complete SAS program flow
    main()
