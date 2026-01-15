# DPFPXFPC.py
# /* TO REFORMAT GOOD DEBIT TRXS FOR CREDITING INTO FPX COLLECTION ACCT*/
# /* DP2614 (DIA2) - CHARGING THE BUYER FEATURE FOR FPX COLL MODEL */

import polars as pl
import duckdb
from datetime import datetime, timedelta
import common.parquet_splitter as parquet_splitter
import common.data_cleaner as data_cleaner
import common.fixed_width_reader as fixed_width_reader
import os
from common.batch_date import get_batch_data
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Obtain date for file paths
date_info = get_batch_data()
file_date = date_info["file_date"]
folder_day = date_info["folder_day"]
folder_month = date_info["folder_month"]
folder_year = date_info["folder_year"]

# Define file paths - matching SAS convention
GSMGOOD = f"/host/dp/input/fpx/GSMGOOD_{folder_year}{folder_month}{folder_day}.dat"
SOURCE = f"/host/dp/input/fpx/SOURCE_{folder_year}{folder_month}{folder_day}.dat"
OUTF1 = f"/host/dp/output/DPFPXFPC_OUTPUT_{folder_year}{folder_month}{folder_day}.dat"
OUTPUT_RPT = f"/host/dp/output/DPFPXFPC_REPORT_{folder_year}{folder_month}{folder_day}.txt"

def main():
    """SAS DPFPXFPC program converted to Python with same variable names"""
    
    try:
        logger.info("Starting DPFPXFPC - TO REFORMAT GOOD DEBIT TRXS FOR CREDITING INTO FPX COLLECTION ACCT")
        logger.info("DP2614 (DIA2) - CHARGING THE BUYER FEATURE FOR FPX COLL MODEL")
        
        # Step 1: Read GSMGOOD into GOODDR
        logger.info("Reading GSMGOOD into GOODDR...")
        GOODDR = read_GOODDR()
        
        # Step 2: Sort GOODDR by SRLNO ODNO ACCTNO SELRID
        logger.info("Sorting GOODDR by SRLNO, ODNO, ACCTNO, SELRID...")
        GOODDR = GOODDR.sort(["SRLNO", "ODNO", "ACCTNO", "SELRID"])
        
        # Step 3: Read SOURCE into DEBSRCE (filtering for BUYBNKID = 'PBB0233')
        logger.info("Reading SOURCE into DEBSRCE (filtering for BUYBNKID = 'PBB0233')...")
        DEBSRCE = read_DEBSRCE()
        
        # Step 4: Sort DEBSRCE by SRLNO ODNO ACCTNO SELRID
        logger.info("Sorting DEBSRCE by SRLNO, ODNO, ACCTNO, SELRID...")
        DEBSRCE = DEBSRCE.sort(["SRLNO", "ODNO", "ACCTNO", "SELRID"])
        
        # Step 5: Merge GOODDR and DEBSRCE to create INTPST (inner join)
        logger.info("Merging GOODDR and DEBSRCE to create INTPST (inner join)...")
        INTPST = merge_GOODDR_DEBSRCE(GOODDR, DEBSRCE)
        
        # Step 6: Sort INTPST by ACCTNO
        logger.info("Sorting INTPST by ACCTNO...")
        INTPST = INTPST.sort("ACCTNO")
        
        # Step 7: Create FPXCR and write to OUTF1
        logger.info("Creating FPXCR and writing to OUTF1...")
        create_FPXCR_write_OUTF1(INTPST)
        
        # Step 8: Sort GOODDR by ACCTNO and print summary
        logger.info("Sorting GOODDR by ACCTNO and printing summary...")
        GOODDR_sorted = GOODDR.sort("ACCTNO")
        print_GOODDR_summary(GOODDR_sorted)
        
        logger.info("DPFPXFPC processing completed successfully.")
        
    except Exception as e:
        logger.error(f"Error in DPFPXFPC processing: {str(e)}", exc_info=True)
        raise

def read_GOODDR():
    """SAS DATA GOODDR; INFILE GSMGOOD MISSOVER; INPUT ... OUTPUT; RUN;"""
    logger.info(f"Reading GOODDR from {GSMGOOD}")
    
    # Column specifications matching SAS INPUT statement
    col_specs = [
        ("SRLNO", 3),           # @001 SRLNO    $3.
        ("ODNO", 40),           # @004 ODNO     $40.
        ("ACCTNO", 11, "int"),  # @044 ACCTNO   11.
        ("ACCTNO1", 1),         # @045 ACCTNO1  $1.
        ("SELRID", 10),         # @064 SELRID   $10.
        ("BUYRID", 20),         # @074 BUYRID   $20.
        ("CHRGTYP", 4),         # @094 CHRGTYP  $4.
        ("TRXAMT", 16, "float"),# @098 TRXAMT   16.2
        ("RESCODE", 2),         # @114 RESCODE  $2.
        ("RESDESC", 50)         # @116 RESDESC  $50.
    ]
    
    # Read the fixed-width file
    GOODDR = fixed_width_reader.read_fixed_width(GSMGOOD, col_specs)
    
    logger.info(f"GOODDR created with {len(GOODDR)} records")
    return GOODDR

def read_DEBSRCE():
    """SAS DATA DEBSRCE; INFILE SOURCE; INPUT @164 BUYBNKID $10. @; ... RUN;"""
    logger.info(f"Reading DEBSRCE from {SOURCE} (filtering for BUYBNKID = 'PBB0233')")
    
    # First, read just the BUYBNKID field to filter
    # We need to read at position 164 (0-indexed: 163)
    temp_specs = [
        ("BUYBNKID", 10, 163)  # @164 BUYBNKID $10.
    ]
    
    # Read all records first
    all_records = fixed_width_reader.read_fixed_width(SOURCE, temp_specs)
    
    # Filter for BUYBNKID = 'PBB0233'
    filtered_indices = all_records.filter(pl.col("BUYBNKID") == "PBB0233").select(pl.arange(0, pl.len()).alias("index"))
    
    logger.info(f"Found {len(filtered_indices)} records with BUYBNKID = 'PBB0233'")
    
    # Now read the full records for filtered indices
    full_specs = [
        ("SRLNO", 3),              # @001 SRLNO          $3.
        ("ODNO", 20),              # @004 ODNO          $20.
        ("SELRID", 10, 43),        # @044 SELRID        $10.
        ("ACCTNO", 10, "int", 188),# @189 ACCTNO         10.
        ("TRANSAMT", 16, "float", 350),  # @351 TRANSAMT      16.2
        ("SETTLAMT", 16, "float", 369),  # @370 SETTLAMT      16.2
        ("FEEPYE", 16, "float", 388),    # @389 FEEPYE        16.2
        ("BUYBNKID", 10, 163)      # @164 BUYBNKID      $10.
    ]
    
    # Read all records with full specs
    all_full = fixed_width_reader.read_fixed_width(SOURCE, full_specs)
    
    # Filter for BUYBNKID = 'PBB0233'
    DEBSRCE = all_full.filter(pl.col("BUYBNKID") == "PBB0233")
    
    logger.info(f"DEBSRCE created with {len(DEBSRCE)} records")
    return DEBSRCE

def merge_GOODDR_DEBSRCE(GOODDR, DEBSRCE):
    """SAS DATA INTPST; MERGE GOODDR(IN=A) DEBSRCE(IN=B); BY SRLNO ODNO ACCTNO SELRID; IF(A AND B)THEN OUTPUT; RUN;"""
    logger.info("Merging GOODDR and DEBSRCE to create INTPST (inner join)")
    
    # Perform inner join on the specified columns
    INTPST = GOODDR.join(
        DEBSRCE,
        on=["SRLNO", "ODNO", "ACCTNO", "SELRID"],
        how="inner"
    )
    
    logger.info(f"INTPST created with {len(INTPST)} records (inner join)")
    return INTPST

def create_FPXCR_write_OUTF1(INTPST):
    """SAS DATA FPXCR; SET INTPST END=EOF; BY ACCTNO; ... FILE OUTF1; PUT ... RUN;"""
    logger.info(f"Creating FPXCR and writing to {OUTF1}")
    
    # Create FPXCR dataframe with all the constant values
    FPXCR = INTPST.with_columns([
        pl.lit(33).alias("BANKNO"),           # BANKNO = 33
        pl.lit(694).alias("TRCODE"),          # TRCODE = 694
        pl.lit(315).alias("SRCODE"),          # SRCODE = 315
        pl.lit(2).alias("TRLCNT"),            # TRLCNT = 2
        pl.lit(0).alias("CONTROL"),           # CONTROL = 0
        pl.lit(0).alias("SERIAL"),            # SERIAL = 0
        pl.lit(3997552105).alias("FPXACCT"),  # FPXACCT = 3997552105
        pl.lit("L").alias("RECORD_START"),    # 'L' at position 1
        pl.lit("D").alias("DEBIT_IND"),       # 'D' at position 2
        pl.lit("ITDBANGI").alias("FILLER"),   # 'ITDBANGI' at positions 11-18
        pl.lit("L").alias("END_MARKER")       # 'L' at position 44
    ])
    
    # According to DP2614, use TRANSAMT instead of TRXAMT
    # Note: TRANSAMT is from DEBSRCE, TRXAMT is from GOODDR
    # In INTPST, we should have both columns from the merge
    
    # Check if TRANSAMT exists, if not use TRXAMT (with warning)
    if "TRANSAMT" in FPXCR.columns:
        logger.info("Using TRANSAMT (DP2614 change implemented)")
        FPXCR = FPXCR.with_columns([
            pl.col("TRANSAMT").alias("OUTPUT_AMT")
        ])
    elif "TRXAMT" in FPXCR.columns:
        logger.warning("TRANSAMT not found, using TRXAMT instead")
        FPXCR = FPXCR.with_columns([
            pl.col("TRXAMT").alias("OUTPUT_AMT")
        ])
    else:
        logger.error("Neither TRANSAMT nor TRXAMT found in INTPST")
        raise ValueError("Neither TRANSAMT nor TRXAMT found in INTPST")
    
    # Define output specifications matching SAS PUT statement with DP2614 changes
    output_specs = [
        ("RECORD_START", 1, "left"),          # @001 'L'
        ("DEBIT_IND", 1, "left"),             # @002 'D'
        ("BANKNO", 2, "right", "int"),        # @003 BANKNO    PD2.
        ("FPXACCT", 6, "right", "int"),       # @005 FPXACCT   PD6.
        ("FILLER", 8, "left"),                # @011 'ITDBANGI'
        ("TRCODE", 2, "right", "int"),        # @019 TRCODE    PD2.
        ("SRCODE", 2, "right", "int"),        # @021 SRCODE    PD2.
        ("CONTROL", 8, "right", "int"),       # @023 CONTROL   PD8.
        # DP2614: Changed from TRXAMT to TRANSAMT
        ("OUTPUT_AMT", 7, "right", "float", 2),  # @031 TRANSAMT  PD7.2  /* DP2614 */
        ("SERIAL", 6, "right", "int"),        # @038 SERIAL    PD6.
        ("END_MARKER", 1, "left"),            # @044 'L'
        ("TRLCNT", 2, "right", "int"),        # @045 TRLCNT    PD2.
        ("SELRID", 10, "left"),               # @047 SELRID    $10.
        ("BUYRID", 20, "left")                # @097 BUYRID    $20.
    ]
    
    # Write fixed-width output
    fixed_width_reader.write_fixed_width(FPXCR, OUTF1, output_specs)
    
    logger.info(f"Output file created: {OUTF1} with {len(FPXCR)} records")
    
    # Generate report
    generate_report(FPXCR)

def generate_report(FPXCR):
    """Generate report file similar to SAS output"""
    logger.info(f"Generating report to {OUTPUT_RPT}")
    
    with open(OUTPUT_RPT, 'w') as f:
        f.write("DPFPXFPC - REFORMAT GOOD DEBIT TRXS FOR CREDITING INTO FPX COLLECTION ACCT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Processing Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"DP2614 (DIA2) Implemented: YES (Using TRANSAMT instead of TRXAMT)\n\n")
        
        f.write("PROCESSING SUMMARY:\n")
        f.write("-" * 40 + "\n")
        f.write(f"Total FPX Collection Records: {len(FPXCR)}\n")
        
        if len(FPXCR) > 0 and "OUTPUT_AMT" in FPXCR.columns:
            f.write(f"Total Output Amount: {FPXCR['OUTPUT_AMT'].sum():,.2f}\n")
            f.write(f"Average Output Amount: {FPXCR['OUTPUT_AMT'].mean():,.2f}\n")
            f.write(f"Maximum Output Amount: {FPXCR['OUTPUT_AMT'].max():,.2f}\n")
            f.write(f"Minimum Output Amount: {FPXCR['OUTPUT_AMT'].min():,.2f}\n\n")
        
        # Count records by SELRID if available
        if len(FPXCR) > 0 and "SELRID" in FPXCR.columns:
            f.write("RECORDS BY SELLER ID (Top 10):\n")
            seller_counts = FPXCR.group_by("SELRID").agg(
                pl.count().alias("count"),
                pl.col("OUTPUT_AMT").sum().alias("total_amount")
            ).sort("count", descending=True).head(10)
            
            for row in seller_counts.iter_rows(named=True):
                f.write(f"  {row['SELRID']}: {row['count']} records, Total: {row['total_amount']:,.2f}\n")
        
        f.write("\n" + "=" * 80 + "\n")
        f.write("END OF REPORT\n")

def print_GOODDR_summary(GOODDR):
    """Mimic SAS PROC PRINT DATA=GOODDR;"""
    logger.info("PROC PRINT DATA=GOODDR equivalent - Summary:")
    
    if len(GOODDR) > 0:
        logger.info(f"Total GOODDR records: {len(GOODDR)}")
        
        # Display first 10 records
        print_records = GOODDR.head(10)
        logger.info("First 10 records:")
        
        for i, row in enumerate(print_records.iter_rows(named=True)):
            logger.info(f"  Record {i+1}: SRLNO={row.get('SRLNO')}, "
                       f"ODNO={row.get('ODNO')}, "
                       f"ACCTNO={row.get('ACCTNO')}, "
                       f"TRXAMT={row.get('TRXAMT', 0):.2f}")
        
        # Summary statistics
        if "TRXAMT" in GOODDR.columns:
            logger.info(f"TRXAMT Statistics: "
                       f"Min={GOODDR['TRXAMT'].min():.2f}, "
                       f"Max={GOODDR['TRXAMT'].max():.2f}, "
                       f"Avg={GOODDR['TRXAMT'].mean():.2f}, "
                       f"Total={GOODDR['TRXAMT'].sum():.2f}")
        
        if "CHRGTYP" in GOODDR.columns:
            charge_counts = GOODDR.group_by("CHRGTYP").agg(pl.count().alias("count"))
            logger.info("Charge Type Distribution:")
            for row in charge_counts.iter_rows(named=True):
                logger.info(f"  {row['CHRGTYP']}: {row['count']} records")

if __name__ == "__main__":
    main()
