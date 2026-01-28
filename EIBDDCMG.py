import polars as pl
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import os
from pathlib import Path
import re

# ==================== SETUP PATHS ====================
BASE_PATH = Path("/path/to/data")
DEPO_PATH = BASE_PATH / "depo"
MIS_PATH = BASE_PATH / "mis"
DEQ_PATH = BASE_PATH / "deq"
IDEQ_PATH = BASE_PATH / "ideq"
RNID_PATH = BASE_PATH / "rnid"
IRNID_PATH = BASE_PATH / "irnid"
OUTPUT_PATH = BASE_PATH / "output"

# Input file paths
EQ_FILE = BASE_PATH / "EQ.parquet"
IEQ_FILE = BASE_PATH / "IEQ.parquet"
DPGIA_FILE = BASE_PATH / "DPGIA.txt"

# Ensure output directory exists
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# ==================== HELPER FUNCTIONS ====================
def add_asa_control(lines, page_length=60):
    """Add ASA carriage control characters to report lines"""
    asa_lines = []
    for i, line in enumerate(lines):
        if i == 0:
            # First line: skip to new page
            asa_lines.append(f"\x0c{line}")
        elif i % page_length == 0:
            # New page
            asa_lines.append(f"\x0c{line}")
        else:
            # Single space
            asa_lines.append(f" {line}")
    return asa_lines

def format_number(value, format_str):
    """Format numbers according to SAS formats"""
    if value is None:
        return " " * 15
    
    if format_str == "COMMA15.":
        return f"{value:15,.0f}"
    elif format_str == "DDMMYY10.":
        if hasattr(value, 'strftime'):
            return value.strftime("%d/%m/%Y").rjust(10)
        return str(value).rjust(10)
    elif format_str == "14.2":
        return f"{value:14.2f}"
    else:
        return str(value).rjust(15)

def read_parquet_file(file_path, duckdb_conn=None):
    """Read parquet file using DuckDB for better performance"""
    if duckdb_conn is None:
        duckdb_conn = duckdb.connect()
    
    query = f"SELECT * FROM read_parquet('{file_path}')"
    result = duckdb_conn.execute(query).fetch_arrow_table()
    return pl.from_arrow(result)

# ==================== MAIN PROCESSING ====================
def main():
    # Initialize DuckDB connection
    conn = duckdb.connect()
    
    # ==================== REPTDATE PROCESSING ====================
    print("Processing REPTDATE...")
    reptdate_df = read_parquet_file(DEPO_PATH / "REPTDATE.parquet", conn)
    
    # Get the first REPTDATE value
    reptdate_val = reptdate_df[0, "REPTDATE"]
    
    # Calculate derived values
    reptdate_dt = reptdate_val.date()
    day_val = reptdate_dt.day
    
    # Determine NOWK based on day
    if day_val == 8:
        NOWK = "1"
    elif day_val == 15:
        NOWK = "2"
    elif day_val == 22:
        NOWK = "3"
    else:
        NOWK = "4"
    
    REPTYY = reptdate_dt.year
    REPTYEAR = str(reptdate_dt.year)[-2:]
    REPTMON = f"{reptdate_dt.month:02d}"
    REPTDAY = f"{day_val:02d}"
    RPDATE = reptdate_dt.strftime("%d%m%Y")
    RDATE = int(reptdate_dt.strftime("%Y%m%d"))
    
    print(f"Report Date: {reptdate_dt}, RDATE: {RDATE}")
    
    # ==================== UTMS PROCESSING ====================
    print("Processing UTMS...")
    # Read EQ file (assuming it's now in parquet format with proper columns)
    utms_df = read_parquet_file(EQ_FILE, conn)
    
    # Filter and process UTMS data
    utms_df = utms_df.with_columns([
        pl.lit(RDATE).alias("REPTDATE")
    ])
    
    # Group by REPTDATE and sum PROCEED
    utms_summary = utms_df.group_by("REPTDATE").agg([
        pl.col("PROCEED").sum().alias("PROCEED")
    ])
    
    # ==================== IUTMS PROCESSING ====================
    print("Processing IUTMS...")
    # Read IEQ file
    ieq_df = read_parquet_file(IEQ_FILE, conn)
    
    # Split into INONFI (first 2 rows) and IFI (from row 3)
    inonfi_df = ieq_df.head(2)
    ifi_df = ieq_df.tail(-2)  # Skip first 2 rows
    
    # Add REPTDATE column
    inonfi_df = inonfi_df.with_columns([
        pl.lit(RDATE).alias("REPTDATE"),
        pl.col("IPROCEED").alias("IPROCEED_ORIG")
    ]).rename({"IPROCEED": "IPROCEFI"})
    
    ifi_df = ifi_df.with_columns([
        pl.lit(RDATE).alias("REPTDATE")
    ])
    
    # Combine both datasets
    iutms_df = pl.concat([inonfi_df, ifi_df])
    
    # Group by REPTDATE and sum
    iutms_summary = iutms_df.group_by("REPTDATE").agg([
        pl.col("IPROCEED_ORIG").sum().alias("IPROCEED"),
        pl.col("IPROCEFI").sum().alias("IPROCEFI")
    ])
    
    # ==================== EQUTMS MERGE ====================
    print("Merging EQUTMS...")
    equtms_df = utms_summary.join(
        iutms_summary,
        on="REPTDATE",
        how="outer"
    )
    
    # ==================== PBGIA PROCESSING ====================
    print("Processing PBGIA...")
    # Read DPGIA as fixed-width text file
    with open(DPGIA_FILE, 'r') as f:
        dpgia_lines = f.readlines()
    
    # Skip first 7 lines (first 7 obs)
    pbgia_data = []
    current_date = None
    line_count = 0
    record_count = 0
    
    for line in dpgia_lines[7:]:  # FIRSTOBS=8
        line = line.rstrip('\n')
        
        if len(line) >= 105:  # Ensure line has enough characters
            # Extract CHECK1 (positions 2-11)
            check1_str = line[1:11].strip()
            # Extract CHECK2 (positions 95-110)
            check2_str = line[94:110].strip()
            
            check1 = None
            check2 = None
            
            # Parse CHECK1 as date
            if check1_str:
                try:
                    # Assuming DD/MM/YYYY format
                    day = int(check1_str[0:2])
                    month = int(check1_str[3:5])
                    year = int(check1_str[6:10])
                    check1 = datetime(year, month, day)
                except:
                    check1 = None
            
            # Parse CHECK2 as float
            if check2_str:
                try:
                    check2 = float(check2_str)
                except:
                    check2 = None
            
            if check1 is not None:
                # This is a date line (N=1)
                line_count = 1
                try:
                    dte = line[1:11]
                    day = int(dte[0:2])
                    month = int(dte[3:5])
                    year = int(dte[6:10])
                    current_date = datetime(year, month, day)
                except:
                    current_date = None
            elif check2 is not None:
                line_count += 1
                
                if line_count == 3:
                    # This is the closing balance line (N=3)
                    try:
                        closbal = float(line[94:110].strip())
                        if current_date:
                            pbgia_data.append({
                                "REPTDATE": int(current_date.strftime("%Y%m%d")),
                                "CLOSBAL": closbal
                            })
                            record_count += 1
                    except:
                        pass
                    line_count = 0
    
    pbgia_df = pl.DataFrame(pbgia_data)
    
    # ==================== UTFX PROCESSING ====================
    print("Processing UTFX...")
    # Read UTFX file for current date
    utfx_file = DEQ_PATH / f"UTFX{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
    utfx_df = read_parquet_file(utfx_file, conn)
    
    # Add REPTDATE and process
    utfx_df = utfx_df.with_columns([
        pl.lit(RDATE).alias("REPTDATE"),
        pl.when(pl.col("DEALTYPE").is_in(["BCD", "BCQ"]))
          .then(pl.col("DEALTYPE"))
          .otherwise(None).alias("VALID_DEALTYPE")
    ]).filter(pl.col("VALID_DEALTYPE").is_not_null())
    
    # Create TOTCPMMD and TOTCDFI columns
    custfiss_exclude = ["02", "03", "07", "10", "12", "81", "82", "83", "84", "01"]
    
    utfx_df = utfx_df.with_columns([
        pl.when(~pl.col("CUSTFISS").is_in(custfiss_exclude))
          .then(pl.col("AMTPAY"))
          .otherwise(None).alias("TOTCPMMD"),
        pl.when(pl.col("CUSTFISS").is_in(custfiss_exclude))
          .then(pl.col("AMTPAY"))
          .otherwise(None).alias("TOTCDFI")
    ])
    
    # Group by REPTDATE
    utfx_summary = utfx_df.group_by("REPTDATE").agg([
        pl.col("TOTCPMMD").sum().alias("TOTCPMMD"),
        pl.col("TOTCDFI").sum().alias("TOTCDFI")
    ])
    
    # ==================== IUTFX PROCESSING ====================
    print("Processing IUTFX...")
    # Read IUTFX file for current date
    iutfx_file = IDEQ_PATH / f"IUTFX{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
    iutfx_df = read_parquet_file(iutfx_file, conn)
    
    # Add REPTDATE and process
    iutfx_df = iutfx_df.with_columns([
        pl.lit(RDATE).alias("REPTDATE"),
        pl.when(pl.col("DEALTYPE").is_in(["BCS", "BCT", "BCW", "BQD"]))
          .then(pl.col("DEALTYPE"))
          .otherwise(None).alias("VALID_DEALTYPE")
    ]).filter(pl.col("VALID_DEALTYPE").is_not_null())
    
    # Create TOTIPMMD and TOTIDFI columns
    iutfx_df = iutfx_df.with_columns([
        pl.when(~pl.col("CUSTFISS").is_in(custfiss_exclude))
          .then(pl.col("AMTPAY"))
          .otherwise(None).alias("TOTIPMMD"),
        pl.when(pl.col("CUSTFISS").is_in(custfiss_exclude))
          .then(pl.col("AMTPAY"))
          .otherwise(None).alias("TOTIDFI")
    ])
    
    # Group by REPTDATE
    iutfx_summary = iutfx_df.group_by("REPTDATE").agg([
        pl.col("TOTIPMMD").sum().alias("TOTIPMMD"),
        pl.col("TOTIDFI").sum().alias("TOTIDFI")
    ])
    
    # ==================== ERATE PROCESSING ====================
    print("Processing ERATE...")
    # Read EFORATE file
    erate_file = MIS_PATH / f"EFORATE{REPTMON}.parquet"
    erate_df = read_parquet_file(erate_file, conn)
    
    # Sort by REPTDATE
    erate_df = erate_df.sort("REPTDATE")
    
    # ==================== RNID PROCESSING ====================
    print("Processing RNID...")
    # Read RNID file
    rnid_file = RNID_PATH / f"RNID{REPTDAY}.parquet"
    rnid_df = read_parquet_file(rnid_file, conn)
    
    # Filter and process
    rnid_df = rnid_df.filter(
        (pl.col("PRODUCT") == 320) &
        (pl.col("NIDSTAT") == "N") &
        (pl.col("CDSTAT") == "A")
    ).with_columns([
        pl.lit(RDATE).alias("REPTDATE"),
        pl.when(pl.col("CUSTCODE").is_in([77, 78]))
          .then(pl.col("CURBAL").abs())
          .otherwise(None).alias("TOTINDV"),
        pl.when(~pl.col("CUSTCODE").is_in([77, 78]))
          .then(pl.col("CURBAL").abs())
          .otherwise(None).alias("TOTNIND")
    ])
    
    # Group by REPTDATE
    rnid_summary = rnid_df.group_by("REPTDATE").agg([
        pl.col("TOTINDV").sum().alias("TOTINDV"),
        pl.col("TOTNIND").sum().alias("TOTNIND")
    ])
    
    # ==================== IRNID PROCESSING ====================
    print("Processing IRNID...")
    # Read IRNID file
    irnid_file = IRNID_PATH / f"RNID{REPTDAY}.parquet"
    irnid_df = read_parquet_file(irnid_file, conn)
    
    # Filter and process
    irnid_df = irnid_df.filter(
        (pl.col("PRODUCT") == 321) &
        (pl.col("NIDSTAT") == "N") &
        (pl.col("CDSTAT") == "A")
    ).with_columns([
        pl.lit(RDATE).alias("REPTDATE"),
        pl.when(pl.col("CUSTCODE").is_in([77, 78]))
          .then(pl.col("CURBAL").abs())
          .otherwise(None).alias("TOIINDV"),
        pl.when(~pl.col("CUSTCODE").is_in([77, 78]))
          .then(pl.col("CURBAL").abs())
          .otherwise(None).alias("TOININD")
    ])
    
    # Group by REPTDATE
    irnid_summary = irnid_df.group_by("REPTDATE").agg([
        pl.col("TOIINDV").sum().alias("TOIINDV"),
        pl.col("TOININD").sum().alias("TOININD")
    ])
    
    # ==================== DCMG MERGE AND CALCULATIONS ====================
    print("Creating DCMG dataset...")
    
    # Create a base dataframe with REPTDATE
    base_df = pl.DataFrame({"REPTDATE": [RDATE]})
    
    # Merge all datasets
    dcmg_df = base_df
    
    # Left join all datasets
    datasets = [
        ("EQUTMS", equtms_df),
        ("PBGIA", pbgia_df),
        ("ERATE", erate_df),
        ("RNID", rnid_summary),
        ("IRNID", irnid_summary),
        ("UTFX", utfx_summary),
        ("IUTFX", iutfx_summary)
    ]
    
    for name, df in datasets:
        if df is not None and len(df) > 0:
            dcmg_df = dcmg_df.join(
                df,
                on="REPTDATE",
                how="left"
            )
    
    # Fill null values with 0 for numeric columns
    numeric_cols = [col for col in dcmg_df.columns if col != "REPTDATE"]
    for col in numeric_cols:
        if col in dcmg_df.columns:
            dcmg_df = dcmg_df.with_columns([
                pl.col(col).fill_null(0)
            ])
    
    # Perform calculations (rounding to nearest 1000 and converting to int)
    dcmg_df = dcmg_df.with_columns([
        (pl.col("PROCEED").round(-3) / 1000).cast(pl.Int64).alias("PROCEED"),
        (pl.col("IPROCEED").round(-3) / 1000).cast(pl.Int64).alias("IPROCEED"),
        (pl.col("IPROCEFI").round(-3) / 1000).cast(pl.Int64).alias("IPROCEFI"),
        (pl.col("CLOSBAL").round(-3) / 1000).cast(pl.Int64).alias("CLOSBAL"),
        (pl.col("TOTINDV").round(-3) / 1000).cast(pl.Int64).alias("TOTINDV"),
        (pl.col("TOTNIND").round(-3) / 1000).cast(pl.Int64).alias("TOTNIND"),
        (pl.col("TOIINDV").round(-3) / 1000).cast(pl.Int64).alias("TOIINDV"),
        (pl.col("TOININD").round(-3) / 1000).cast(pl.Int64).alias("TOININD"),
        (pl.col("TOTCPMMD").round(-3) / 1000).cast(pl.Int64).alias("TOTCPMMD"),
        (pl.col("TOTIPMMD").round(-3) / 1000).cast(pl.Int64).alias("TOTIPMMD"),
        (pl.col("TOTCDFI").round(-3) / 1000).cast(pl.Int64).alias("TOTCDFI"),
        (pl.col("TOTIDFI").round(-3) / 1000).cast(pl.Int64).alias("TOTIDFI")
    ])
    
    # Calculate derived columns
    dcmg_df = dcmg_df.with_columns([
        (pl.col("PROCEED") + pl.col("IPROCEED")).alias("TOTPROCD"),
        (pl.col("TOTINDV") + pl.col("TOTNIND")).alias("TOTRNID"),
        (pl.col("TOIINDV") + pl.col("TOININD")).alias("TOIRNID"),
        (pl.col("TOTRNID") + pl.col("TOIRNID")).alias("TOTANID"),
        (pl.col("TOTCPMMD") + pl.col("TOTIPMMD")).alias("TOTNPMMD"),
        (pl.col("TOTCDFI") + pl.col("TOTIDFI")).alias("TOTNDFI")
    ])
    
    # Sort by REPTDATE
    dcmg_df = dcmg_df.sort("REPTDATE")
    
    # ==================== APPEND TO MONTHLY DATASET ====================
    print("Appending to monthly dataset...")
    monthly_file = MIS_PATH / f"DCMG{REPTMON}.parquet"
    
    if REPTDAY == "01":
        # Create new monthly file
        dcmg_df.write_parquet(monthly_file)
    else:
        # Append to existing file
        if monthly_file.exists():
            existing_df = read_parquet_file(monthly_file, conn)
            combined_df = pl.concat([dcmg_df, existing_df])
            
            # Remove duplicates based on REPTDATE
            combined_df = combined_df.unique(subset=["REPTDATE"], keep="first")
            combined_df = combined_df.sort("REPTDATE")
            
            combined_df.write_parquet(monthly_file)
        else:
            dcmg_df.write_parquet(monthly_file)
    
    # ==================== GENERATE REPORT ====================
    print("Generating report...")
    
    # Read the complete monthly dataset for reporting
    report_df = read_parquet_file(monthly_file, conn)
    
    # Filter for current RDATE
    report_df = report_df.filter(pl.col("REPTDATE") == RDATE)
    
    # Prepare report lines
    report_lines = []
    
    # Add titles
    report_date_str = datetime.strptime(str(RDATE), "%Y%m%d").strftime("%d/%m/%Y")
    
    report_lines.append(" " * 40 + "REPORT ID : DMMISR1G")
    report_lines.append(" " * 45 + "PUBLIC BANK BERHAD")
    report_lines.append(" " * 30 + "SALES ADMINISTRATION & SUPPORT")
    report_lines.append(" " * 35 + "BANK'S TOTAL DEPOSITS (RM '000)")
    report_lines.append(" " * 45 + f"AS AT {RPDATE[:2]}/{RPDATE[2:4]}/{RPDATE[4:]}")
    report_lines.append("")
    report_lines.append("=" * 155)
    
    # Create header
    header = (
        f"{'DATE':10} "
        f"{'NID-C':>15} "
        f"{'NID-I':>15} "
        f"{'TOTAL':>15} "
        f"{'NID-I(FI)':>15} "
        f"{'INDV':>15} "
        f"{'NON-INDV':>15} "
        f"{'TOTAL':>15} "
        f"{'INDV':>15} "
        f"{'NON-INDV':>15} "
        f"{'TOTAL':>15} "
        f"{'TOTAL':>15}"
    )
    report_lines.append(header)
    
    subheader1 = (
        f"{'':10} "
        f"{'(NON-FI)':>15} "
        f"{'(NON-FI)':>15} "
        f"{'NIDS':>15} "
        f"{'(PBB ONLY)':>15} "
        f"{'RETAIL':>15} "
        f"{'RETAIL':>15} "
        f"{'RETAIL':>15} "
        f"{'RETAIL':>15} "
        f"{'RETAIL':>15} "
        f"{'RETAIL':>15} "
        f"{'RETAIL':>15}"
    )
    report_lines.append(subheader1)
    
    subheader2 = (
        f"{'':10} "
        f"{'(PROCEEDS)':>15} "
        f"{'(PROCEEDS)':>15} "
        f"{'(CONV + ISL)':>15} "
        f"{'(PROCEEDS)':>15} "
        f"{'NID':>15} "
        f"{'NID':>15} "
        f"{'NID':>15} "
        f"{'NIDC':>15} "
        f"{'NIDC':>15} "
        f"{'NIDC':>15} "
        f"{'NID & NIDC':>15}"
    )
    report_lines.append(subheader2)
    
    subheader3 = (
        f"{'':10} "
        f"{'':>15} "
        f"{'':>15} "
        f"{'':>15} "
        f"{'':>15} "
        f"{'(CONV)':>15} "
        f"{'(CONV)':>15} "
        f"{'(CONV)':>15} "
        f"{'(ISL)':>15} "
        f"{'(ISL)':>15} "
        f"{'(ISL)':>15} "
        f"{'(CONV + ISL)':>15}"
    )
    report_lines.append(subheader3)
    
    report_lines.append("-" * 155)
    
    # Process second part of header
    header2 = (
        f"{'PMMD &':>15} "
        f"{'PMMD':>15} "
        f"{'TOTAL':>15} "
        f"{'DFI & FI':>15} "
        f"{'DFI & FI':>15} "
        f"{'TOTAL':>15} "
        f"{'GIA':>15} "
        f"{'GIA':>14} "
        f"{'GIA':>14}"
    )
    report_lines.append(header2)
    
    subheader4 = (
        f"{'PQMMD':>15} "
        f"{'':>15} "
        f"{'PMMD':>15} "
        f"{'PMMD &':>15} "
        f"{'PMMD':>15} "
        f"{'DFI & FI':>15} "
        f"{'BALANCE':>15} "
        f"{'CLSG PRICE':>14} "
        f"{'CLSG PRICE':>14}"
    )
    report_lines.append(subheader4)
    
    subheader5 = (
        f"{'(CONV)':>15} "
        f"{'(ISL)':>15} "
        f"{'(CONV + ISL)':>15} "
        f"{'PQMMD':>15} "
        f"{'(ISL)':>15} "
        f"{'PMMD':>15} "
        f"{'IN RM':>15} "
        f"{'RM/G':>14} "
        f"{'RM/G':>14}"
    )
    report_lines.append(subheader5)
    
    subheader6 = (
        f"{'':>15} "
        f"{'':>15} "
        f"{'':>15} "
        f"{'(CONV)':>15} "
        f"{'':>15} "
        f"{'(CONV + ISL)':>15} "
        f"{'(SPOTRATE)':>15} "
        f"{'(SELLING)':>14} "
        f"{'(BUYING)':>14}"
    )
    report_lines.append(subheader6)
    
    report_lines.append("-" * 155)
    
    # Add data row
    if len(report_df) > 0:
        row = report_df.row(0)
        
        # Format the data row
        date_str = format_number(
            datetime.strptime(str(row[0]), "%Y%m%d"),
            "DDMMYY10."
        )
        
        data_line = (
            f"{date_str} "
            f"{format_number(row[1], 'COMMA15.')} "  # PROCEED
            f"{format_number(row[2], 'COMMA15.')} "  # IPROCEED
            f"{format_number(row[12], 'COMMA15.')} "  # TOTPROCD
            f"{format_number(row[3], 'COMMA15.')} "  # IPROCEFI
            f"{format_number(row[4], 'COMMA15.')} "  # TOTINDV
            f"{format_number(row[5], 'COMMA15.')} "  # TOTNIND
            f"{format_number(row[13], 'COMMA15.')} "  # TOTRNID
            f"{format_number(row[6], 'COMMA15.')} "  # TOIINDV
            f"{format_number(row[7], 'COMMA15.')} "  # TOININD
            f"{format_number(row[14], 'COMMA15.')} "  # TOIRNID
            f"{format_number(row[15], 'COMMA15.')} "  # TOTANID
            f"{format_number(row[8], 'COMMA15.')} "  # TOTCPMMD
            f"{format_number(row[9], 'COMMA15.')} "  # TOTIPMMD
            f"{format_number(row[16], 'COMMA15.')} "  # TOTNPMMD
            f"{format_number(row[10], 'COMMA15.')} "  # TOTCDFI
            f"{format_number(row[11], 'COMMA15.')} "  # TOTIDFI
            f"{format_number(row[17], 'COMMA15.')} "  # TOTNDFI
            f"{format_number(row[18], 'COMMA15.')} "  # CLOSBAL
            f"{format_number(row[19], '14.2')} "  # SELLRATE
            f"{format_number(row[20], '14.2')} "  # BUYRATE
        )
        report_lines.append(data_line)
    
    report_lines.append("=" * 155)
    
    # Add ASA carriage control
    asa_report = add_asa_control(report_lines)
    
    # Write report to file
    report_file = OUTPUT_PATH / f"DCMG_REPORT_{RPDATE}.txt"
    with open(report_file, 'w') as f:
        for line in asa_report:
            f.write(line + '\n')
    
    print(f"Report generated: {report_file}")
    
    # Close DuckDB connection
    conn.close()
    
    print("Processing complete!")

if __name__ == "__main__":
    main()
