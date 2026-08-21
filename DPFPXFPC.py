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

out_dir   = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDCIA"
temp_dir  = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDCIA/temp"

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
# Define schema for DPFL file
dp_schema = [
    ("TICKETNO", 0, 7, "str"),
    ("BRANCH", 7, 12, "str"),
    ("CUSTNAME", 12, 38, "str"),
    ("NEWIC", 38, 58, "str"),
    ("SALESID", 58, 66, "str"),
    ("CUSTCODE", 66, 71, "int"),
    ("INVCURRAC", 71, 82, "str"),
    ("ALTCURRAC", 82, 93, "str"),
    ("INVCURR", 93, 96, "str"),
    ("ALTCURR", 96, 99, "str"),
    ("INVAMT", 99, 112, "float"),
    ("TRYY", 112, 116, "str"),
    ("TRMM", 117, 119, "str"),
    ("TRDD", 120, 122, "str"),
    ("STYY", 122, 126, "str"),
    ("STMM", 127, 129, "str"),
    ("STDD", 130, 132, "str"),
    ("FIYY", 132, 136, "str"),
    ("FIMM", 137, 139, "str"),
    ("FIDD", 140, 142, "str"),
    ("MTYY", 142, 146, "str"),
    ("MTMM", 147, 149, "str"),
    ("MTDD", 150, 152, "str"),
    ("TENOR", 152, 155, "int"),
    ("STRIKERT", 155, 168, "float"),
    ("DCIRT", 168, 177, "float"),
    ("ACCINT", 177, 192, "float"),
    ("ROLLOVER", 192, 193, "str"),
    ("CONVERTIND", 193, 194, "str"),
    ("DEALERID", 194, 202, "str"),
    ("MANAGERID", 202, 210, "str")
]

rows = []
with open(dpfl_file) as f:
    next(f)  # skip header
    for line in f:
        if not line.strip():
            continue
            
        row = {}
        for col, start, end, typ in dp_schema:
            raw = line[start:end].strip()
            if typ == "int":
                row[col] = int(raw) if raw else None
            elif typ == "float":
                row[col] = float(raw) if raw else None
            else:
                row[col] = raw
        
        # Create date fields
        try:
            row["TRADEDT"] = dt.date(int(row["TRYY"]), int(row["TRMM"]), int(row["TRDD"]))
            row["STARTDT"] = dt.date(int(row["STYY"]), int(row["STMM"]), int(row["STDD"]))
            row["FIXINGDT"] = dt.date(int(row["FIYY"]), int(row["FIMM"]), int(row["FIDD"]))
            row["MATDT"] = dt.date(int(row["MTYY"]), int(row["MTMM"]), int(row["MTDD"]))
        except (ValueError, TypeError):
            row["TRADEDT"] = row["STARTDT"] = row["FIXINGDT"] = row["MATDT"] = None
        
        # Apply CUSTCODE transformation
        if row["CUSTCODE"] is not None:
            custcode = row["CUSTCODE"]
            if 100 <= custcode <= 999:
                row["CUSTCODE"] = int(str(custcode)[1:3])
            elif 1000 <= custcode <= 9999:
                row["CUSTCODE"] = int(str(custcode)[2:4])
            elif 10000 <= custcode <= 99999:
                row["CUSTCODE"] = int(str(custcode)[3:5])
        
        rows.append(row)

# Create DPST DataFrame
dpst = pl.DataFrame(rows).select([
    "TICKETNO", "NEWIC", "SALESID", "CUSTCODE", "INVCURRAC", "ALTCURRAC",
    "ROLLOVER", "CONVERTIND", "DEALERID", "MANAGERID", "CUSTNAME", "ACCINT"
])

# Clean data
dpst = dpst.filter(pl.col("TICKETNO") != "")
dpst = dpst.with_columns([
    pl.col("TICKETNO").str.replace_all(" ", ""),
    pl.col("INVCURRAC").str.replace_all(" ", "").cast(pl.Utf8)
])

print(f"DPST records: {len(dpst)}")

# -------------------------
# STEP 4: Parse EQFL (pipe-delimited)
# -------------------------
eq_columns = [
    "CUSTICKETNO", "TICKETNO", "BRANCH", "CUSTNAME", "DEALID", "CUSTYPE",
    "RESIDENCE_COUNTRY", "CUSTOMER_MNEMONIC", "CUSTOMER_LOC", "CUSTOMER_TYPE",
    "PRODUCT", "INVCURR", "ALTCURR", "INVAMT", "INVAMTRM", "ALTAMT",
    "TRADEDTX", "STARTDTX", "FIXDTX", "MATDTX", "STOPDTX", "TENOR",
    "STRIKERT", "SPOTRT", "DCIRT", "DCI_DAILY_INT", "DCI_INT_ACCRUED",
    "ACCINTEQ", "MMRT", "RPTSPOTRT", "PREMREC", "PREMPAID", "PROFIT",
    "PROFITMYR", "UNWINDCOST", "STATIND", "NEWDEAL", "TRAN_TYPE"
]

eqtn_raw = pl.read_csv(
    eqfl_file, 
    separator="|", 
    has_header=False,
    skip_rows=1,
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

# Parse dates
def parse_sas_date(date_str):
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

# Apply transformations
eqtn = eqtn.with_columns([
    pl.col("ACCINTEQ").abs().alias("ACCINTEQ"),
    pl.col("DCI_INT_ACCRUED").abs().alias("DCI_INT_ACCRUED"),
    pl.col("PREMPAID").abs().alias("PREMPAID"),
])

# Map status
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

# Select needed columns
eqtn = eqtn.select([
    "TICKETNO", "BRANCH", "PRODUCT", "INVCURR", "ALTCURR", "CUSTICKETNO",
    "INVAMT", "ALTAMT", "TRADEDT", "STARTDT", "FIXINGDT", "MATDT", "TENOR",
    "STRIKERT", "SPOTRT", "DCIRT", "MMRT", "PREMREC", "PREMPAID",
    "UNWINDCOST", "NEWDEAL", "STATUSIND", "STOPDT"
])

# Clean TICKETNO
eqtn = eqtn.with_columns(pl.col("TICKETNO").str.replace_all(" ", ""))

print(f"EQTN records: {len(eqtn)}")

# -------------------------
# STEP 5: Merge DPST & EQTN
# -------------------------
dcid = dpst.join(eqtn, on="TICKETNO", how="inner")
dcid = dcid.filter(pl.col("NEWDEAL").is_in(["O", "N"])).drop("STOPDT")

print(f"DCID records after merge and filter: {len(dcid)}")

# -------------------------
# STEP 6: Join reference tables
# -------------------------
if len(dcid) > 0:
    # Read reference files
    print("Reading reference files...")
    
    # CA file
    ca_df, _ = pyreadstat.read_sas7bdat(
        ca_file.format(REPTYEAR=REPTYEAR, REPTMON=REPTMON, REPTDAY=REPTDAY)
    )
    ca = pl.from_pandas(ca_df).select(["ACCTNO", "CUSTFISS"]).with_columns([
        pl.col("CUSTFISS").cast(pl.Utf8).str.slice(0, 2).cast(pl.Int64).alias("CUSTCODE2"),
        pl.col("ACCTNO").cast(pl.Utf8).str.replace_all(" ", "").alias("INVCURRAC2")
    ]).select(["INVCURRAC2", "CUSTCODE2"])
    
    # SA file
    sa_df, _ = pyreadstat.read_sas7bdat(
        sa_file.format(REPTYEAR=REPTYEAR, REPTMON=REPTMON, REPTDAY=REPTDAY)
    )
    sa = pl.from_pandas(sa_df).select(["ACCTNO", "CUSTCODE"]).with_columns([
        pl.col("CUSTCODE").cast(pl.Int64).alias("CUSTCODE2"),
        pl.col("ACCTNO").cast(pl.Utf8).str.replace_all(" ", "").alias("INVCURRAC2")
    ]).select(["INVCURRAC2", "CUSTCODE2"])
    
    # FCY file
    fcy_df, _ = pyreadstat.read_sas7bdat(
        fcy_file.format(REPTYEAR=REPTYEAR, REPTMON=REPTMON, REPTDAY=REPTDAY)
    )
    fcy = pl.from_pandas(fcy_df).select(["ACCTNO", "CUSTCD"]).with_columns([
        pl.col("CUSTCD").cast(pl.Int64).alias("CUSTCODE2"),
        pl.col("ACCTNO").cast(pl.Utf8).str.replace_all(" ", "").alias("INVCURRAC2")
    ]).select(["INVCURRAC2", "CUSTCODE2"])
    
    # Combine reference data
    dpdata = pl.concat([sa, ca, fcy])
    print(f"Reference data records: {len(dpdata)}")
    
    # Join with main data
    dcid2 = dcid.join(dpdata, left_on="INVCURRAC", right_on="INVCURRAC2", how="left")
    
    # Update CUSTCODE
    if "CUSTCODE2" in dcid2.columns:
        dcid2 = dcid2.with_columns(
            pl.when(pl.col("CUSTCODE2").is_not_null())
              .then(pl.col("CUSTCODE2"))
              .otherwise(pl.col("CUSTCODE"))
              .alias("CUSTCODE")
        )
        cols_to_drop = [col for col in ["INVCURRAC2", "CUSTCODE2"] if col in dcid2.columns]
        if cols_to_drop:
            dcid2 = dcid2.drop(cols_to_drop)
    
    print(f"Final records: {len(dcid2)}")
    
    # -------------------------
    # STEP 7: Save results
    # -------------------------
    # Ensure output directories exist
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(temp_dir, exist_ok=True)
    
    # Save as Parquet
    parquet_file = f"{out_file}.parquet"
    dcid2.write_parquet(parquet_file)
    print(f"✓ Saved Parquet: {parquet_file}")
    
    # Save as SAS7BDAT using saspy
    try:
        import saspy
        import pandas as pd
        
        print("Connecting to SAS...")
        sas = saspy.SASsession()
        
        # Convert to pandas for SAS
        dcid2_pd = dcid2.to_pandas()
        
        # Upload to SAS
        print("Uploading data to SAS...")
        sas_df = sas.df2sd(dcid2_pd, f"work_dcid{REPTMON}{REPTDAY}")
        
        # Save to DCI library
        print("Saving to DCI library...")
        result = sas.submit(f"""
            LIBNAME DCI '{out_dir}';
            DATA DCI.DCID{REPTMON}{REPTDAY};
                SET work_dcid{REPTMON}{REPTDAY};
            RUN;
        """)
        
        # Check for errors in DCI save
        if "ERROR" in str(result.get('LOG', '')).upper():
            print("⚠ Warning: Errors found in SAS LOG for DCI library save")
            print("SAS LOG:", result.get('LOG', 'No log available'))
        else:
            print(f"✓ Saved SAS dataset: DCI.DCID{REPTMON}{REPTDAY}")
        
        # Save to TEMP library (create directory first)
        print("Saving to TEMP library...")
        result = sas.submit(f"""
            LIBNAME TEMP '{temp_dir}';
            DATA TEMP.DCID{REPTYEAR}{REPTMON}{REPTDAY};
                SET work_dcid{REPTMON}{REPTDAY};
            RUN;
        """)
        
        # Check for errors in TEMP save
        if "ERROR" in str(result.get('LOG', '')).upper():
            print("⚠ Warning: Errors found in SAS LOG for TEMP library save")
            print("SAS LOG:", result.get('LOG', 'No log available'))
        else:
            print(f"✓ Saved SAS dataset: TEMP.DCID{REPTYEAR}{REPTMON}{REPTDAY}")
        
        # Close SAS session
        sas.endsas()
        print("SAS session closed.")
        
    except ImportError:
        print("⚠ saspy not available. Only Parquet file created.")
    except Exception as e:
        print(f"⚠ Error saving SAS file: {e}")
        print("Only Parquet file created.")
    
    print("\n✅ Processing completed successfully!")
    print(f"Output files:")
    print(f"  - {parquet_file}")
    
else:
    print("\n⚠ No data to process!")
