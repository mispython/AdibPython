# DPFPXMR1.py
# /*  FPX DDS MONTHLY REPORT FOR OPCC'S VERIFICATION           */

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
INPUTF1 = f"/host/dp/input/fpx/INPUTF1_{folder_year}{folder_month}{folder_day}.dat"
CORPFILE = f"/host/dp/input/fpx/CORPFILE_{folder_year}{folder_month}{folder_day}.dat"
OUTPUT_RPT = f"/host/dp/output/DPFPXMR1_REPORT_{folder_year}{folder_month}{folder_day}.txt"

def main():
    """SAS DPFPXMR1 program converted to Python with same variable names"""
    
    try:
        logger.info("Starting DPFPXMR1 - FPX DDS MONTHLY REPORT FOR OPCC'S VERIFICATION")
        
        # Step 1: Create RPTDATE (date calculations)
        logger.info("Creating RPTDATE...")
        RPTDATE = create_RPTDATE()
        
        # Print RPTDATE summary (matching SAS PROC PRINT)
        print_RPTDATE_summary(RPTDATE)
        
        # Step 2: Read SOURCE1
        logger.info(f"Reading SOURCE1 from {INPUTF1}...")
        SOURCE1 = read_SOURCE1()
        
        # Step 3: Sort SOURCE1 by SELLACNO
        logger.info("Sorting SOURCE1 by SELLACNO...")
        SOURCE1 = SOURCE1.sort("SELLACNO")
        
        # Step 4: Read CORPFL
        logger.info(f"Reading CORPFL from {CORPFILE}...")
        CORPFL = read_CORPFL()
        
        # Step 5: Sort CORPFL by SELLACNO
        logger.info("Sorting CORPFL by SELLACNO...")
        CORPFL = CORPFL.sort("SELLACNO")
        
        # Step 6: Merge SOURCE1 and CORPFL to create MRG1
        logger.info("Merging SOURCE1 and CORPFL to create MRG1...")
        MRG1 = merge_SOURCE1_CORPFL(SOURCE1, CORPFL)
        
        # Step 7: Sort MRG1 by TRXDATE, CORPCODE, TSTATUS
        logger.info("Sorting MRG1 by TRXDATE, CORPCODE, TSTATUS...")
        MRG1 = MRG1.sort(["TRXDATE", "CORPCODE", "TSTATUS"])
        
        # Step 8: Create SEG1 and SEG2 with accumulators
        logger.info("Creating SEG1 and SEG2 with accumulators...")
        SEG1, SEG2 = create_SEG1_SEG2(MRG1)
        
        # Step 9: Sort SEG1 and SEG2
        logger.info("Sorting SEG1 and SEG2...")
        SEG1 = SEG1.sort(["CORPCODE", "TSTATUS", "TRXDATE", "SELLACNO"])
        SEG2 = SEG2.sort(["TRXDATE", "CORPCODE", "TSTATUS"])
        
        # Step 10: Generate TOTALALL report
        logger.info(f"Generating TOTALALL report to {OUTPUT_RPT}...")
        generate_TOTALALL_report(SEG2, RPTDATE)
        
        logger.info("DPFPXMR1 processing completed successfully.")
        
    except Exception as e:
        logger.error(f"Error in DPFPXMR1 processing: {str(e)}", exc_info=True)
        raise

def create_RPTDATE():
    """SAS DATA RPTDATE; DT = TODAY(); ... RUN;"""
    logger.info("Creating RPTDATE with date calculations")
    
    # Get current date and time
    current_dt = datetime.now()
    yesterday_dt = current_dt - timedelta(days=1)
    
    # Create dictionary with all date fields
    RPTDATE = {
        "DT": current_dt,
        "DD": current_dt.strftime("%d"),  # Z2. format
        "MM": current_dt.strftime("%m"),  # Z2. format
        "CCYY": current_dt.strftime("%Y"),  # Z4. format
        "YDT": yesterday_dt,
        "YDD": yesterday_dt.strftime("%d"),  # Z2. format
        "YMM": yesterday_dt.strftime("%m"),  # Z2. format
        "YCCYY": yesterday_dt.strftime("%Y"),  # Z4. format
        "YY": current_dt.strftime("%y"),  # Last 2 digits of year
        "DTODAY": current_dt.strftime("%Y-%m-%d"),  # YYYY-MM-DD
        "DTYEST": yesterday_dt.strftime("%Y-%m-%d"),  # YYYY-MM-DD
        "TN": current_dt.time(),
        "STARTIME": current_dt.strftime("%H:%M"),  # HH:MM
        # SAS macro variables equivalents
        "DAY": current_dt.strftime("%d"),
        "STTIME": current_dt.strftime("%H:%M"),
        "MONTH": current_dt.strftime("%m"),
        "YEAR": current_dt.strftime("%y"),
        "CCYY_MACRO": current_dt.strftime("%Y"),
        "DTTDY": current_dt.strftime("%Y-%m-%d"),
        "DTYST": yesterday_dt.strftime("%Y-%m-%d")
    }
    
    logger.info(f"RPTDATE created: DTODAY={RPTDATE['DTODAY']}, DTYEST={RPTDATE['DTYEST']}")
    return RPTDATE

def print_RPTDATE_summary(RPTDATE):
    """Mimic SAS PROC PRINT DATA=RPTDATE; TITLE 'DATE';"""
    logger.info("PROC PRINT DATA=RPTDATE:")
    logger.info(f"  DT: {RPTDATE['DT']}")
    logger.info(f"  DD: {RPTDATE['DD']}, MM: {RPTDATE['MM']}, CCYY: {RPTDATE['CCYY']}")
    logger.info(f"  YDT: {RPTDATE['YDT']}")
    logger.info(f"  YDD: {RPTDATE['YDD']}, YMM: {RPTDATE['YMM']}, YCCYY: {RPTDATE['YCCYY']}")
    logger.info(f"  YY: {RPTDATE['YY']}")
    logger.info(f"  DTODAY: {RPTDATE['DTODAY']}")
    logger.info(f"  DTYEST: {RPTDATE['DTYEST']}")
    logger.info(f"  STARTIME: {RPTDATE['STARTIME']}")

def read_SOURCE1():
    """SAS DATA SOURCE1; INFILE INPUTF1; INPUT ... OUTPUT SOURCE1; RUN;"""
    logger.info(f"Reading SOURCE1 from {INPUTF1}")
    
    # Define schema for fixed-width reading (matching SAS INPUT statement)
    schema = [
        (0, 2, "SELLBNCD"),     # @001   SELLBNCD        2.
        (2, 10, "SELLEXID"),    # @003   SELLEXID      $10.
        (12, 10, "SELLID"),     # @013   SELLID        $10.
        (22, 10, "SELLACNO"),   # @023   SELLACNO       10.
        (32, 20, "SELLORNO"),   # @033   SELLORNO      $20.
        (52, 10, "BUYBNKID"),   # @053   BUYBNKID      $10.
        (62, 6, "BUYBRCD"),     # @063   BUYBRCD       PD6.
        (68, 20, "BUYACNO"),    # @069   BUYACNO       $20.
        (68, 10, "ACCTNO"),     # @069   ACCTNO         10.
        (88, 40, "BUYNAME"),    # @089   BUYNAME       $40.
        (128, 10, "TRXDATE"),   # @129   TRXDATE       $10.
        (128, 4, "TRXYYYY"),    # @129   TRXYYYY         4.
        (132, 1, "DASH1"),      # @133   DASH           $1.
        (133, 2, "TRXMM"),      # @134   TRXMM           2.
        (135, 1, "DASH2"),      # @136   DASH           $1.
        (136, 2, "TRXDD"),      # @137   TRXDD           2.
        (138, 3, "TRXCURR"),    # @139   TRXCURR        $3.
        (141, 7, "TRXAMNT"),    # @142   TRXAMNT       PD7.2
        (141, 7, "TRXAMNTX"),   # @142   TRXAMNTX      PD7.
        (148, 4, "PRCYYYY"),    # @149   PRCYYYY         4.
        (152, 1, "DASH3"),      # @153   DASH           $1.
        (153, 2, "PRCMM"),      # @154   PRCMM           2.
        (155, 1, "DASH4"),      # @156   DASH           $1.
        (156, 2, "PRCDD"),      # @157   PRCDD           2.
        (158, 2, "TSTATUS"),    # @159   TSTATUS        $2.
        (160, 2, "RESPCODE"),   # @161   RESPCODE      PD2.
        (162, 10, "IDFILE"),    # @163   IDFILE        $10.
        (172, 4, "FEEPYR"),     # @173   FEEPYR        PD4.
        (176, 4, "FEEPYE"),     # @177   FEEPYE        PD4.
        (180, 4, "FEEPYRF"),    # @181   FEEPYRF       PD4.
        (184, 4, "FEEPYEF"),    # @185   FEEPYEF       PD4.
        (188, 20, "BUYREF1"),   # @189   BUYREF1       $20.
        (208, 20, "BUYREF2")    # @209   BUYREF2       $20.
    ]
    
    # Read using fixed_width_reader
    SOURCE1 = fixed_width_reader.read(INPUTF1, schema)
    
    # Convert numeric columns
    SOURCE1 = convert_numeric_SOURCE1(SOURCE1)
    
    # Extract SELLID for macro variable (simulating CALL SYMPUT)
    if len(SOURCE1) > 0:
        sellid_value = SOURCE1[0, "SELLID"] if "SELLID" in SOURCE1.columns else ""
        logger.info(f"SELLID macro variable set to: {sellid_value}")
    
    logger.info(f"SOURCE1 created with {len(SOURCE1)} records")
    return SOURCE1

def convert_numeric_SOURCE1(df):
    """Convert numeric columns in SOURCE1"""
    numeric_cols = ["SELLBNCD", "SELLACNO", "BUYBRCD", "ACCTNO", "TRXYYYY", 
                   "TRXMM", "TRXDD", "PRCYYYY", "PRCMM", "PRCDD", "RESPCODE"]
    
    for col in numeric_cols:
        if col in df.columns:
            df = df.with_columns([
                pl.col(col).cast(pl.Int64).alias(col)
            ])
    
    # Convert PD (packed decimal) fields
    if "TRXAMNT" in df.columns:
        df = df.with_columns([
            (pl.col("TRXAMNT").cast(pl.Float64) / 100).alias("TRXAMNT")  # PD7.2
        ])
    if "TRXAMNTX" in df.columns:
        df = df.with_columns([
            pl.col("TRXAMNTX").cast(pl.Float64).alias("TRXAMNTX")  # PD7
        ])
    
    pd4_cols = ["FEEPYR", "FEEPYE", "FEEPYRF", "FEEPYEF"]
    for col in pd4_cols:
        if col in df.columns:
            df = df.with_columns([
                pl.col(col).cast(pl.Float64).alias(col)
            ])
    
    return df

def read_CORPFL():
    """SAS DATA CORPFL; INFILE CORPFILE; FORMAT ... INPUT ... RUN;"""
    logger.info(f"Reading CORPFL from {CORPFILE}")
    
    # Define schema for fixed-width reading
    schema = [
        (10, 3, "INTERID"),     # @011  INTERID      $3.
        (13, 10, "SELLACNO"),   # @014  SELLACNO     10.
        (23, 15, "CORPNM1"),    # @024  CORPNM1     $15.
        (38, 3, "CORPCODE")     # @039  CORPCODE     $3.
    ]
    
    # Read using fixed_width_reader
    CORPFL = fixed_width_reader.read(CORPFILE, schema)
    
    # Convert numeric columns
    if "SELLACNO" in CORPFL.columns:
        CORPFL = CORPFL.with_columns([
            pl.col("SELLACNO").cast(pl.Int64).alias("SELLACNO")
        ])
    
    # Apply CORPNAME logic from SAS
    CORPFL = apply_CORPNAME_logic(CORPFL)
    
    # Filter for INTERID = 'EDP'
    CORPFL = CORPFL.filter(pl.col("INTERID") == "EDP")
    
    logger.info(f"CORPFL created with {len(CORPFL)} records")
    return CORPFL

def apply_CORPNAME_logic(df):
    """Apply SAS SELECT logic for CORPNAME"""
    # Start with CORPNM1 as CORPTMP
    df = df.with_columns([
        pl.col("CORPNM1").alias("CORPTMP"),
        pl.lit("").alias("CORPNM2"),
        pl.col("CORPNM1").alias("CORPNAME")  # Default
    ])
    
    # Apply the SELECT logic from SAS
    for i, row in enumerate(df.iter_rows(named=True)):
        sellacno = str(row.get("SELLACNO", ""))
        corpnm2 = ""
        
        # SAS SELECT statement
        if sellacno == '3144687430':
            corpnm2 = 'FPX ING FUNDS BERHAD'
        elif sellacno == '3133491431':
            corpnm2 = 'FPX ALLIANZ LIFE INSURANCE'
        elif sellacno == '3999204719':
            corpnm2 = 'FPX TELEKOM MALAYSIA BHD'
        elif sellacno == '3098872530':
            corpnm2 = 'TEST PBB 02'
        elif sellacno == '3098872433':
            corpnm2 = 'TEST PBB 01'
        elif sellacno == '3127887731':
            corpnm2 = 'FPX EXERTAINMENT MALAYSIA SDN BHD'
        elif sellacno == '3119351620':
            corpnm2 = 'FPX NU SKIN (M) SDN BHD'
        elif sellacno == '3122120534':
            corpnm2 = 'FPX IHM RISK PROTECTION'
        elif sellacno == '3139000719':
            corpnm2 = 'FPX GHL TRANSACT SDN BHD'
        elif sellacno == '3072728219':
            corpnm2 = 'FPX NEP DIAMOND MARKETING'
        elif sellacno == '3140184926':
            corpnm2 = 'FPX GHL PAYMENTS SDN BHD'
        
        # Update the row
        if corpnm2:
            df = df.with_columns([
                pl.when(pl.arange(0, pl.len()) == i)
                .then(pl.lit(corpnm2))
                .otherwise(pl.col("CORPNM2"))
                .alias("CORPNM2")
            ])
    
    # Final CORPNAME logic: if CORPNM2 is not empty, use it, otherwise use CORPTMP
    df = df.with_columns([
        pl.when(pl.col("CORPNM2").str.strip() != "")
        .then(pl.col("CORPNM2"))
        .otherwise(pl.col("CORPTMP"))
        .alias("CORPNAME")
    ])
    
    return df

def merge_SOURCE1_CORPFL(SOURCE1, CORPFL):
    """SAS DATA MRG1; MERGE SOURCE1(IN=A) CORPFL(IN=B); BY SELLACNO; IF A THEN OUTPUT MRG1; RUN;"""
    logger.info("Merging SOURCE1 and CORPFL to create MRG1")
    
    # Convert SELLACNO to same type for merging
    if "SELLACNO" in SOURCE1.columns:
        SOURCE1 = SOURCE1.with_columns([
            pl.col("SELLACNO").cast(pl.Utf8).alias("SELLACNO_STR")
        ])
    
    if "SELLACNO" in CORPFL.columns:
        CORPFL = CORPFL.with_columns([
            pl.col("SELLACNO").cast(pl.Utf8).alias("SELLACNO_STR")
        ])
    
    # Perform left join (SOURCE1 IN=A, CORPFL IN=B, only output if A)
    MRG1 = SOURCE1.join(
        CORPFL,
        left_on="SELLACNO_STR",
        right_on="SELLACNO_STR",
        how="left"
    )
    
    # Drop the temporary column
    MRG1 = MRG1.drop("SELLACNO_STR")
    
    logger.info(f"MRG1 created with {len(MRG1)} records")
    return MRG1

def create_SEG1_SEG2(MRG1):
    """SAS DATA SEG1 SEG2; SET MRG1; BY TRXDATE CORPCODE TSTATUS; ... RUN;"""
    logger.info("Creating SEG1 and SEG2 with accumulators")
    
    # Initialize accumulators
    SUC1CNT = 0
    REJ1CNT = 0
    SUC1AMT = 0.0
    REJ1AMT = 0.0
    
    # Process each row
    processed_rows = []
    for i, row in enumerate(MRG1.iter_rows(named=True)):
        tstatus = row.get("TSTATUS", "")
        trxamnt = float(row.get("TRXAMNT", 0))
        stat1 = ""
        
        if tstatus == 'SC':
            stat1 = 'SUCCESSFUL'
            SUC1CNT += 1
            SUC1AMT += trxamnt
        elif tstatus == 'BR':
            stat1 = 'REJECTED'
            REJ1CNT += 1
            REJ1AMT += trxamnt
        
        # Add calculated fields to row
        row_with_calc = dict(row)
        row_with_calc["STAT1"] = stat1
        row_with_calc["SUC1CNT"] = SUC1CNT
        row_with_calc["REJ1CNT"] = REJ1CNT
        row_with_calc["SUC1AMT"] = SUC1AMT
        row_with_calc["REJ1AMT"] = REJ1AMT
        
        # Filter out TSTATUS = 'IS' (as in SAS)
        if tstatus != 'IS':
            processed_rows.append(row_with_calc)
    
    # Convert to DataFrame
    if processed_rows:
        SEG1 = pl.DataFrame(processed_rows)
        SEG2 = SEG1.clone()  # Both segments get the same data initially
    else:
        SEG1 = pl.DataFrame()
        SEG2 = pl.DataFrame()
    
    logger.info(f"SEG1 and SEG2 created with {len(processed_rows)} records")
    logger.info(f"Accumulators: SUC1CNT={SUC1CNT}, REJ1CNT={REJ1CNT}, SUC1AMT={SUC1AMT:.2f}, REJ1AMT={REJ1AMT:.2f}")
    
    return SEG1, SEG2

def generate_TOTALALL_report(SEG2, RPTDATE):
    """Generate the main report (SAS DATA TOTALALL; ... RUN;)"""
    logger.info("Generating TOTALALL report")
    
    if len(SEG2) == 0:
        generate_empty_report(OUTPUT_RPT, RPTDATE)
        return
    
    # Open output file
    with open(OUTPUT_RPT, 'w') as f:
        # Initialize variables
        pageline = []
        page_no = 1
        linecnt = 0
        trn = len(SEG2)
        
        # Group by TRXDATE and CORPCODE
        # First, sort SEG2 as in SAS
        SEG2_sorted = SEG2.sort(["TRXDATE", "CORPCODE", "TSTATUS"])
        
        # Get unique dates
        unique_dates = SEG2_sorted["TRXDATE"].unique().to_list()
        
        # Initialize accumulators
        cnt = 0
        corpcnt = 0
        trxcnt_total = 0
        trxamt_total = 0.0
        totcnt = 0
        totamt = 0.0
        suc1cnt_total = 0
        suc1amt_total = 0.0
        rej1cnt_total = 0
        rej1amt_total = 0.0
        
        # Track current group accumulators
        trxcbr = 0
        trxabr = 0.0
        trxcsc = 0
        trxasc = 0.0
        
        current_trxdate = None
        current_corpcode = None
        current_corpname = None
        current_sellacno = None
        
        # Process each row
        for i, row in enumerate(SEG2_sorted.iter_rows(named=True)):
            trxdate = row.get("TRXDATE", "")
            corpcode = row.get("CORPCODE", "")
            tstatus = row.get("TSTATUS", "")
            sellacno = row.get("SELLACNO", "")
            corpname = row.get("CORPNAME", "")
            trxamnt = float(row.get("TRXAMNT", 0))
            
            # Check for page break
            if linecnt >= 52:
                write_page_header(f, page_no, RPTDATE)
                page_no += 1
                linecnt = 5
            
            # New date group
            if trxdate != current_trxdate:
                if current_trxdate is not None and current_corpcode is not None:
                    # Write out the previous corporate summary
                    write_corp_summary(f, cnt, current_sellacno, current_corpname, 
                                      trxcbr + trxcsc, trxabr + trxasc,
                                      trxcsc, trxasc, trxcbr, trxabr)
                    cnt += 1
                    corpcnt += 1
                
                # Reset for new date
                current_trxdate = trxdate
                trxcbr = 0
                trxabr = 0.0
                trxcsc = 0
                trxasc = 0.0
                
                # Write date header
                f.write(f"\n\n\nPAYMENT DATE : {trxdate}\n")
                f.write("NO  ACCOUNT NO   CORPORATE NAME          TOTAL CNT    TOTAL AMT     SUCC CNT   SUCC AMT    REJ CNT    REJ AMT\n")
                f.write("--- -----------  --------------------   ---------   ------------   --------   ---------   --------   ---------\n")
                linecnt += 5
            
            # New corporate within same date
            if corpcode != current_corpcode:
                if current_corpcode is not None and current_trxdate == trxdate:
                    # Write out the previous corporate summary
                    write_corp_summary(f, cnt, current_sellacno, current_corpname,
                                      trxcbr + trxcsc, trxabr + trxasc,
                                      trxcsc, trxasc, trxcbr, trxabr)
                    cnt += 1
                    corpcnt += 1
                    trxcbr = 0
                    trxabr = 0.0
                    trxcsc = 0
                    trxasc = 0.0
                
                current_corpcode = corpcode
                current_corpname = corpname
                current_sellacno = sellacno
            
            # Accumulate by status
            if tstatus == 'BR':
                trxcbr += 1
                trxabr += trxamnt
                rej1cnt_total += 1
                rej1amt_total += trxamnt
            elif tstatus == 'SC':
                trxcsc += 1
                trxasc += trxamnt
                suc1cnt_total += 1
                suc1amt_total += trxamnt
            
            totcnt += 1
            totamt += trxamnt
        
        # Write the last corporate summary
        if current_corpcode is not None:
            write_corp_summary(f, cnt, current_sellacno, current_corpname,
                              trxcbr + trxcsc, trxabr + trxasc,
                              trxcsc, trxasc, trxcbr, trxabr)
            corpcnt += 1
        
        # Write totals
        f.write("\n========================================\n")
        f.write(f"TOTAL NO OF CORP       : {corpcnt:7d}\n")
        f.write(f"TOTAL PAYMENT COUNT    : {totcnt:7d}\n")
        f.write(f"TOTAL PAYMENT AMOUNT   : {totamt:16,.2f}\n")
        f.write(f"TOTAL SUCCESSFUL COUNT : {suc1cnt_total:7d}\n")
        f.write(f"TOTAL SUCCESSFUL AMOUNT: {suc1amt_total:16,.2f}\n")
        f.write(f"TOTAL REJECTED COUNT   : {rej1cnt_total:7d}\n")
        f.write(f"TOTAL REJECTED AMOUNT  : {rej1amt_total:16,.2f}\n")
        f.write("========================================\n")
        
        # Write signature section
        current_time = datetime.now().strftime("%H:%M:%S")
        f.write("\n\n\nCHECKED BY\n")
        f.write("\nOS/CO : ____________________    INITIAL : ____________________    DATE/TIME : ____________________\n")
        f.write("\nSOS   : ____________________    INITIAL : ____________________    DATE/TIME : ____________________\n")
        f.write("\nSCOS  : ____________________    INITIAL : ____________________    DATE/TIME : ____________________    TIME : " + current_time + "\n")
    
    logger.info(f"Report generated: {OUTPUT_RPT}")

def write_page_header(f, page_no, RPTDATE):
    """Write page header (NEWPAGE section in SAS)"""
    report_date = datetime.now().strftime("%d/%m/%y")
    
    f.write("REPORT NO   : FPXDDS/MTHLY/001     P U B L I C  B A N K  B E R H A D                        PAGE        : {:4d}\n".format(page_no))
    f.write("PROGRAM ID  : DPFPXDR1             MONTHLY SUMMARY REPORT OF ALL FPX DSS PROCESSED PAYMENTS REPORT DATE : {}\n".format(report_date))
    f.write("\n" * 3)  # Three blank lines

def write_corp_summary(f, cnt, sellacno, corpname, trxcnt, trxamt, trxcsc, trxasc, trxcbr, trxabr):
    """Write corporate summary line"""
    cnt_str = f"{cnt:03d}"
    sellacno_str = str(sellacno)[:10].ljust(10)
    corpname_str = (corpname or "")[:20].ljust(20)
    trxcnt_str = f"{trxcnt:7d}"
    trxamt_str = f"{trxamt:13,.2f}".rjust(13)
    trxcsc_str = f"{trxcsc:7d}"
    trxasc_str = f"{trxasc:12,.2f}".rjust(12)
    trxcbr_str = f"{trxcbr:7d}"
    trxabr_str = f"{trxabr:12,.2f}".rjust(12)
    
    f.write(f"{cnt_str} {sellacno_str} {corpname_str} {trxcnt_str} {trxamt_str} {trxcsc_str} {trxasc_str} {trxcbr_str} {trxabr_str}\n")

def generate_empty_report(output_file, RPTDATE):
    """Generate report when there are no transactions"""
    logger.info("Generating empty report (no transactions)")
    
    with open(output_file, 'w') as f:
        # Write page header
        write_page_header(f, 1, RPTDATE)
        
        # Write "NIL TRANSACTION" message
        f.write("\n" * 10)  # Blank lines
        f.write(" " * 49 + "------- NIL TRANSACTION ---------\n")
        f.write("\n" * 2)  # Blank lines
        
        # Write generation date
        current_date = datetime.now().strftime("%d/%m/%Y")
        f.write(" " * 40 + "*** THIS REPORT IS GENERATED ON " + current_date + " ***\n")

if __name__ == "__main__":
    main()
