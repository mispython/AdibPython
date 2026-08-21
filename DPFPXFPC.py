import polars as pl
import pyreadstat
import datetime as dt
import os

# -------------------------
# CONFIG
# -------------------------
dpfl_file = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDDCIA/dpfl.txt"
eqfl_file = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDDCIA/eqfl.txt"

ca_file   = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDDCIA/ca{REPTYEAR}{REPTMON}{REPTDAY}.sas7bdat"
sa_file   = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDDCIA/sa{REPTYEAR}{REPTMON}{REPTDAY}.sas7bdat"
fcy_file  = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDDCIA/fcy{REPTYEAR}{REPTMON}{REPTDAY}.sas7bdat"

out_dir   = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDDCIA"

# -------------------------
# STEP 1: Report Date (yesterday)
# -------------------------
reptdate = dt.date.today() - dt.timedelta(days=1)
RDATE = reptdate.strftime("%d%m%y")
REPTYEAR = reptdate.strftime("%y")
REPTMON  = reptdate.strftime("%m")
REPTDAY  = reptdate.strftime("%d")

print(f"Report Date: {RDATE} ({REPTDAY}-{REPTMON}-{REPTYEAR})")

# Output file naming
out_file = f"{out_dir}/dcid{REPTMON}{REPTDAY}"

# -------------------------
# STEP 2: Read header dates from text files
# -------------------------
# Read DPFL header (first line)
with open(dpfl_file) as f:
    hdr = f.readline()
    yy = int(hdr[0:4])
    mm = int(hdr[5:7])
    dd = int(hdr[8:10])
    DPDATE = dt.date(yy, mm, dd)

# Read EQFL header (first line)
with open(eqfl_file) as f:
    hdr = f.readline()
    dd = int(hdr[19:21])
    mm = int(hdr[21:23])
    yy = int(hdr[23:27])
    EQDATE = dt.date(yy, mm, dd)

print("DPDATE:", DPDATE.strftime("%d%m%y"), "EQDATE:", EQDATE.strftime("%d%m%y"))

# -------------------------
# Only run if dates match
# -------------------------
if DPDATE.strftime("%d%m%y") != RDATE:
    raise SystemExit(f"❌ DPDATE {DPDATE.strftime('%d%m%y')} does not match RDATE {RDATE}")

# -------------------------
# STEP 3: Parse DPFL fixed-width file → DPST
# -------------------------
# Define schema for DPFL file (positions are 1-based in SAS, 0-based in Python)
dp_schema = [
    ("TICKETNO", 0, 7, "str"),      # 1-7
    ("BRANCH", 7, 12, "str"),       # 8-12
    ("CUSTNAME", 12, 38, "str"),    # 13-38
    ("NEWIC", 38, 58, "str"),       # 39-58
    ("SALESID", 58, 66, "str"),     # 59-66
    ("CUSTCODE", 66, 71, "int"),    # 67-71
    ("INVCURRAC", 71, 82, "str"),   # 72-82
    ("ALTCURRAC", 82, 93, "str"),   # 83-93
    ("INVCURR", 93, 96, "str"),     # 94-96
    ("ALTCURR", 96, 99, "str"),     # 97-99
    ("INVAMT", 99, 112, "float"),   # 100-112
    ("TRYY", 112, 116, "str"),      # 113-116
    ("TRMM", 117, 119, "str"),      # 118-119
    ("TRDD", 120, 122, "str"),      # 121-122
    ("STYY", 122, 126, "str"),      # 123-126
    ("STMM", 127, 129, "str"),      # 128-129
    ("STDD", 130, 132, "str"),      # 131-132
    ("FIYY", 132, 136, "str"),      # 133-136
    ("FIMM", 137, 139, "str"),      # 138-139
    ("FIDD", 140, 142, "str"),      # 141-142
    ("MTYY", 142, 146, "str"),      # 143-146
    ("MTMM", 147, 149, "str"),      # 148-149
    ("MTDD", 150, 152, "str"),      # 151-152
    ("TENOR", 152, 155, "int"),     # 153-155
    ("STRIKERT", 155, 168, "float"), # 156-168
    ("DCIRT", 168, 177, "float"),   # 169-177
    ("ACCINT", 177, 192, "float"),  # 178-192
    ("ROLLOVER", 192, 193, "str"),  # 193
    ("CONVERTIND", 193, 194, "str"), # 194
    ("DEALERID", 194, 202, "str"),  # 195-202
    ("MANAGERID", 202, 210, "str")  # 203-210
]

rows = []
with open(dpfl_file) as f:
    next(f)  # skip header (FIRSTOBS=2)
    for line in f:
        if not line.strip():  # Skip empty lines
            continue
            
        row = {}
        # Parse fixed-width fields
        for col, start, end, typ in dp_schema:
            raw = line[start:end].strip()
            if typ == "int":
                row[col] = int(raw) if raw else None
            elif typ == "float":
                row[col] = float(raw) if raw else None
            else:
                row[col] = raw
        
        # Create date fields (SAS MDY function)
        try:
            row["TRADEDT"] = dt.date(int(row["TRYY"]), int(row["TRMM"]), int(row["TRDD"]))
            row["STARTDT"] = dt.date(int(row["STYY"]), int(row["STMM"]), int(row["STDD"]))
            row["FIXINGDT"] = dt.date(int(row["FIYY"]), int(row["FIMM"]), int(row["FIDD"]))
            row["MATDT"] = dt.date(int(row["MTYY"]), int(row["MTMM"]), int(row["MTDD"]))
        except (ValueError, TypeError):
            row["TRADEDT"] = row["STARTDT"] = row["FIXINGDT"] = row["MATDT"] = None
        
        # Apply CUSTCODE transformation (SAS logic)
        if row["CUSTCODE"] is not None:
            custcode = row["CUSTCODE"]
            if 0 <= custcode <= 99:
                pass  # Keep as is
            elif 100 <= custcode <= 999:
                row["CUSTCODE"] = int(str(custcode)[1:3])  # SUBSTR(PUT(CUSTCD,3.),2,2)
            elif 1000 <= custcode <= 9999:
                row["CUSTCODE"] = int(str(custcode)[2:4])  # SUBSTR(PUT(CUSTCD,4.),3,2)
            elif 10000 <= custcode <= 99999:
                row["CUSTCODE"] = int(str(custcode)[3:5])  # SUBSTR(PUT(CUSTCD,5.),4,2)
        
        rows.append(row)

# Create DPST DataFrame with selected columns
dpst = pl.DataFrame(rows).select([
    "TICKETNO", "NEWIC", "SALESID", "CUSTCODE", "INVCURRAC", "ALTCURRAC",
    "ROLLOVER", "CONVERTIND", "DEALERID", "MANAGERID", "CUSTNAME", "ACCINT"
])

# Clean TICKETNO and INVCURRAC - remove empty strings and filter
dpst = dpst.filter(pl.col("TICKETNO") != "")
dpst = dpst.with_columns([
    pl.col("TICKETNO").str.replace_all(" ", ""),
    pl.col("INVCURRAC").str.replace_all(" ", "").cast(pl.Utf8)
])

print(f"DPST records: {len(dpst)}")
print(f"DPST TICKETNO sample: {dpst['TICKETNO'].head(5).to_list()}")
print(f"DPST INVCURRAC dtype: {dpst['INVCURRAC'].dtype}")

# -------------------------
# STEP 4: Parse EQFL (pipe-delimited)
# -------------------------
# Read EQFL file
eq_columns = [
    "CUSTICKETNO", "TICKETNO", "BRANCH", "CUSTNAME", "DEALID", "CUSTYPE",
    "RESIDENCE_COUNTRY", "CUSTOMER_MNEMONIC", "CUSTOMER_LOC", "CUSTOMER_TYPE",
    "PRODUCT", "INVCURR", "ALTCURR", "INVAMT", "INVAMTRM", "ALTAMT",
    "TRADEDTX", "STARTDTX", "FIXDTX", "MATDTX", "STOPDTX", "TENOR",
    "STRIKERT", "SPOTRT", "DCIRT", "DCI_DAILY_INT", "DCI_INT_ACCRUED",
    "ACCINTEQ", "MMRT", "RPTSPOTRT", "PREMREC", "PREMPAID", "PROFIT",
    "PROFITMYR", "UNWINDCOST", "STATIND", "NEWDEAL", "TRAN_TYPE"
]

# Read pipe-delimited file with schema overrides for numeric columns
eqtn_raw = pl.read_csv(
    eqfl_file, 
    separator="|", 
    has_header=False,
    skip_rows=1,  # FIRSTOBS=2
    new_columns=eq_columns,
    schema_overrides={
        "INVAMT": pl.Float64,
        "INVAMTRM": pl.Float64,
        "ALTAMT": pl.Float64,
        "TENOR": pl.Float64,
        "STRIKERT": pl.Float64,
        "SPOTRT": pl.Float64,
        "DCIRT": pl.Float64,
        "DCI_DAILY_INT": pl.Float64,
        "DCI_INT_ACCRUED": pl.Float64,
        "ACCINTEQ": pl.Float64,
        "MMRT": pl.Float64,
        "RPTSPOTRT": pl.Float64,
        "PREMREC": pl.Float64,
        "PREMPAID": pl.Float64,
        "PROFIT": pl.Float64,
        "PROFITMYR": pl.Float64,
        "UNWINDCOST": pl.Float64
    }
)

# Parse dates (SAS INPUT with YYMMDD10. format)
def parse_sas_date(date_str):
    """Parse date in YYMMDD10. format (e.g., 2024-01-15 or 24-01-15)"""
    if not date_str or date_str.strip() == "":
        return None
    try:
        return dt.datetime.strptime(date_str.strip(), "%Y-%m-%d").date()
    except ValueError:
        try:
            return dt.datetime.strptime(date_str.strip(), "%y-%m-%d").date()
        except ValueError:
            return None

eqtn = eqtn_raw.with_columns([
    pl.col("TRADEDTX").map_elements(parse_sas_date).alias("TRADEDT"),
    pl.col("STARTDTX").map_elements(parse_sas_date).alias("STARTDT"),
    pl.col("FIXDTX").map_elements(parse_sas_date).alias("FIXINGDT"),
    pl.col("MATDTX").map_elements(parse_sas_date).alias("MATDT"),
    pl.col("STOPDTX").map_elements(parse_sas_date).alias("STOPDT"),
])

# Apply absolute value transformations (now numeric columns are properly typed)
eqtn = eqtn.with_columns([
    pl.col("ACCINTEQ").abs().alias("ACCINTEQ"),
    pl.col("DCI_INT_ACCRUED").abs().alias("DCI_INT_ACCRUED"),
    pl.col("PREMPAID").abs().alias("PREMPAID"),
])

# Map STATIND to STATUSIND
status_map = {
    "New": "N ",
    "Outstanding": "OS",
    "Mature": "M",
    "Premature": "P",
    "Cancelled": "C"
}
eqtn = eqtn.with_columns(
    pl.col("STATIND").map_elements(lambda x: status_map.get(x.strip(), "")).alias("STATUSIND")
)

# Select needed columns for EQTN
eqtn = eqtn.select([
    "TICKETNO", "BRANCH", "PRODUCT", "INVCURR", "ALTCURR", "CUSTICKETNO",
    "INVAMT", "ALTAMT", "TRADEDT", "STARTDT", "FIXINGDT", "MATDT", "TENOR",
    "STRIKERT", "SPOTRT", "DCIRT", "MMRT", "PREMREC", "PREMPAID",
    "UNWINDCOST", "NEWDEAL", "STATUSIND", "STOPDT"
])

# Clean TICKETNO in EQTN
eqtn = eqtn.with_columns(pl.col("TICKETNO").str.replace_all(" ", ""))

print(f"EQTN records: {len(eqtn)}")
print(f"EQTN TICKETNO sample: {eqtn['TICKETNO'].head(5).to_list()}")

# -------------------------
# STEP 5: Merge DPST & EQTN by TICKETNO
# -------------------------
# Check for common TICKETNO values
common_tickets = set(dpst["TICKETNO"].to_list()) & set(eqtn["TICKETNO"].to_list())
print(f"Common TICKETNO values: {len(common_tickets)}")

if len(common_tickets) == 0:
    print("WARNING: No common TICKETNO values found between DPST and EQTN!")
    print("DPST sample:", dpst["TICKETNO"].head(10).to_list())
    print("EQTN sample:", eqtn["TICKETNO"].head(10).to_list())
    
    # Try alternative join on CUSTICKETNO
    if "CUSTICKETNO" in eqtn.columns:
        print("Trying join on CUSTICKETNO...")
        # Clean CUSTICKETNO in EQTN
        eqtn = eqtn.with_columns(pl.col("CUSTICKETNO").str.replace_all(" ", ""))
        dcid = dpst.join(
            eqtn.rename({"CUSTICKETNO": "TICKETNO_ALT"}),
            left_on="TICKETNO",
            right_on="TICKETNO_ALT",
            how="inner"
        ).drop("TICKETNO_ALT")
    else:
        dcid = pl.DataFrame()  # Empty DataFrame if no join possible
else:
    dcid = dpst.join(eqtn, on="TICKETNO", how="inner")

if len(dcid) > 0:
    dcid = dcid.filter(pl.col("NEWDEAL").is_in(["O", "N"])).drop("STOPDT")
    print(f"DCID records after merge and filter: {len(dcid)}")
else:
    print("WARNING: No matching records found after join!")

# -------------------------
# STEP 6: Join CA / SA / FCY reference tables
# -------------------------
# Only proceed if we have data
if len(dcid) > 0:
    # Read SAS files with pyreadstat
    print("Reading CA file...")
    ca_df, ca_meta = pyreadstat.read_sas7bdat(
        ca_file.format(REPTYEAR=REPTYEAR, REPTMON=REPTMON, REPTDAY=REPTDAY)
    )
    # Convert ACCTNO to string to match INVCURRAC
    ca = pl.from_pandas(ca_df).select(["ACCTNO", "CUSTFISS"]).with_columns([
        pl.col("CUSTFISS").cast(pl.Utf8).str.slice(0, 2).cast(pl.Int64).alias("CUSTCODE2"),
        pl.col("ACCTNO").cast(pl.Utf8).str.replace_all(" ", "").alias("INVCURRAC2")
    ]).select(["INVCURRAC2", "CUSTCODE2"])

    print("Reading SA file...")
    sa_df, sa_meta = pyreadstat.read_sas7bdat(
        sa_file.format(REPTYEAR=REPTYEAR, REPTMON=REPTMON, REPTDAY=REPTDAY)
    )
    sa = pl.from_pandas(sa_df).select(["ACCTNO", "CUSTCODE"]).with_columns([
        pl.col("CUSTCODE").cast(pl.Int64).alias("CUSTCODE2"),
        pl.col("ACCTNO").cast(pl.Utf8).str.replace_all(" ", "").alias("INVCURRAC2")
    ]).select(["INVCURRAC2", "CUSTCODE2"])

    print("Reading FCY file...")
    fcy_df, fcy_meta = pyreadstat.read_sas7bdat(
        fcy_file.format(REPTYEAR=REPTYEAR, REPTMON=REPTMON, REPTDAY=REPTDAY)
    )
    fcy = pl.from_pandas(fcy_df).select(["ACCTNO", "CUSTCD"]).with_columns([
        pl.col("CUSTCD").cast(pl.Int64).alias("CUSTCODE2"),
        pl.col("ACCTNO").cast(pl.Utf8).str.replace_all(" ", "").alias("INVCURRAC2")
    ]).select(["INVCURRAC2", "CUSTCODE2"])

    # Combine all reference data (now all CUSTCODE2 are Int64, INVCURRAC2 are strings)
    dpdata = pl.concat([sa, ca, fcy])

    print(f"Reference data records: {len(dpdata)}")
    print(f"Reference INVCURRAC2 dtype: {dpdata['INVCURRAC2'].dtype}")

    # Join with main data - both keys are now strings
    dcid2 = dcid.join(dpdata, left_on="INVCURRAC", right_on="INVCURRAC2", how="left")

    # Update CUSTCODE if found in reference tables
    dcid2 = dcid2.with_columns(
        pl.when(pl.col("CUSTCODE2").is_not_null())
          .then(pl.col("CUSTCODE2"))
          .otherwise(pl.col("CUSTCODE"))
          .alias("CUSTCODE")
    ).drop(["INVCURRAC2", "CUSTCODE2"])

    print(f"Final records: {len(dcid2)}")

    # -------------------------
    # STEP 7: Save results
    # -------------------------
    # Save as Parquet
    parquet_file = f"{out_file}.parquet"
    dcid2.write_parquet(parquet_file)
    print(f"Saved Parquet: {parquet_file}")

    # Save as SAS7BDAT using saspy
    try:
        import saspy
        import pandas as pd
        
        # Initialize SAS session
        sas = saspy.SASsession()
        
        # Convert Polars DataFrame to pandas for SAS
        dcid2_pd = dcid2.to_pandas()
        
        # Upload to SAS
        sas_df = sas.df2sd(dcid2_pd, f"dcid{REPTMON}{REPTDAY}")
        
        # Save as SAS7BDAT in DCI library
        sas.submit(f"""
            DATA DCI.DCID{REPTMON}{REPTDAY};
                SET dcid{REPTMON}{REPTDAY};
            RUN;
        """)
        
        # Also save to TEMP library
        sas.submit(f"""
            DATA TEMP.DCID{REPTYEAR}{REPTMON}{REPTDAY};
                SET dcid{REPTMON}{REPTDAY};
            RUN;
        """)
        
        # Close SAS session
        sas.endsas()
        print(f"Saved SAS datasets: DCI.DCID{REPTMON}{REPTDAY} and TEMP.DCID{REPTYEAR}{REPTMON}{REPTDAY}")
        
    except ImportError:
        print("saspy not available. Only Parquet file created.")
        print("To save as SAS7BDAT, install saspy and configure SAS connection.")
    except Exception as e:
        print(f"Error saving SAS file: {e}")
        print("Only Parquet file created.")

    print("\nProcessing complete!")
    print(f"Output files:")
    print(f"  - {parquet_file}")
else:
    print("\nNo data to process. Check TICKETNO formats between DPFL and EQFL files.")
    print("DPST TICKETNO format:", dpst["TICKETNO"].head(3).to_list())
    print("EQTN TICKETNO format:", eqtn["TICKETNO"].head(3).to_list())
