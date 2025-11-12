import polars as pl
from pathlib import Path
import datetime
import sys

# Configuration
mnitb_path = Path("MNITB")
imnitb_path = Path("IMNITB")
walk_path = Path("WALK")
mis_path = Path("SAP/PBB/MIS")
output_path = Path("output")
output_path.mkdir(exist_ok=True)

# DATA REPTDATE;
reptdate_df = pl.read_parquet(mnitb_path / "REPTDATE.parquet")
first_reptdate = reptdate_df.row(0)['REPTDATE']

# Extract date parameters
REPTYY = first_reptdate.strftime('%y')  # YEAR2.
REPTYEAR = first_reptdate.strftime('%Y')  # YEAR4.
REPTMON = f"{first_reptdate.month:02d}"  # Z2.
REPTDAY = f"{first_reptdate.day:02d}"  # Z2.
RDATE = first_reptdate.strftime('%d%m%y')  # Z5. equivalent
XDATE = first_reptdate.strftime('%d%m%y')  # DDMMYY8.

print(f"REPTYY: {REPTYY}, REPTYEAR: {REPTYEAR}, REPTMON: {REPTMON}")
print(f"REPTDAY: {REPTDAY}, RDATE: {RDATE}, XDATE: {XDATE}")

# DATA TDATE;
TDATE = datetime.date.today() - datetime.timedelta(days=1)
TDATE_STR = TDATE.strftime('%d%m%y')  # Z5. equivalent

print(f"TDATE: {TDATE_STR}")

# LIBNAME MIS "SAP.PBB.MIS.D&REPTYEAR" DISP=OLD;
mis_year_path = mis_path / f"D{REPTYEAR}"
mis_year_path.mkdir(parents=True, exist_ok=True)

# %INC PGM(PBBDPFMT); - Assuming format definitions are handled elsewhere
# Load format mappings (you'll need to define these based on your DDCUSTCD and FDCUSTCD formats)
try:
    from PBBDPFMT import DDCUSTCD, FDCUSTCD
except ImportError:
    print("NOTE: PBBDPFMT formats not found, using empty mappings")
    DDCUSTCD = {}
    FDCUSTCD = {}

# DATA CAFY;
current_df = pl.read_parquet(mnitb_path / "CURRENT.parquet")
icurrent_df = pl.read_parquet(imnitb_path / "CURRENT.parquet").rename({"CURBAL": "ICURBAL"})

# Combine datasets
cafy_df = pl.concat([current_df, icurrent_df])

# Apply filters and transformations
cafy_df = cafy_df.filter(
    (pl.col("CURCODE") != "MYR") &
    (pl.col("PRODUCT").is_between(400, 411) | 
     pl.col("PRODUCT") == 413 |
     pl.col("PRODUCT").is_between(420, 434) |
     pl.col("PRODUCT").is_between(440, 444) |
     pl.col("PRODUCT").is_between(450, 454))
).with_columns([
    # CUSTCD = PUT(CUSTCODE,DDCUSTCD.)
    pl.col("CUSTCODE").map_dict(DDCUSTCD).alias("CUSTCD"),
    # Initialize columns
    pl.lit(0.0).alias("CURBAL1"),
    pl.lit(0.0).alias("ICURBAL1"),
    pl.lit(0.0).alias("FCAFIC"),
    pl.lit(0.0).alias("FCAFII"),
    pl.lit(0.0).alias("FCAIDC"),
    pl.lit(0.0).alias("FCAIDI"),
    pl.lit(int(RDATE)).alias("REPTDATE")
]).with_columns([
    # IF CUSTCD IN (02,03,07,10,12,81,82,83,84) OR PRODUCT = 413 THEN DO;
    pl.when(
        pl.col("CUSTCD").is_in(["02", "03", "07", "10", "12", "81", "82", "83", "84"]) |
        (pl.col("PRODUCT") == 413)
    ).then(pl.struct([
        pl.col("CURBAL").alias("FCAFIC"),
        pl.col("ICURBAL").alias("FCAFII")
    ])).otherwise(pl.struct([
        pl.col("CURBAL").alias("CURBAL1"),
        pl.col("ICURBAL").alias("ICURBAL1")
    ])).alias("temp_assign")
]).with_columns([
    pl.col("temp_assign").struct.field("FCAFIC").alias("FCAFIC"),
    pl.col("temp_assign").struct.field("FCAFII").alias("FCAFII"),
    pl.col("temp_assign").struct.field("CURBAL1").alias("CURBAL1"),
    pl.col("temp_assign").struct.field("ICURBAL1").alias("ICURBAL1")
]).with_columns([
    # IF PRODUCT NE 413 AND CUSTCODE IN (77,78,95,96) THEN DO;
    pl.when(
        (pl.col("PRODUCT") != 413) & 
        pl.col("CUSTCODE").is_in([77, 78, 95, 96])
    ).then(pl.struct([
        pl.col("CURBAL").alias("FCAIDC"),
        pl.col("ICURBAL").alias("FCAIDI")
    ])).otherwise(pl.struct([
        pl.col("FCAIDC"),
        pl.col("FCAIDI")
    ])).alias("temp_assign2")
]).with_columns([
    pl.col("temp_assign2").struct.field("FCAIDC").alias("FCAIDC"),
    pl.col("temp_assign2").struct.field("FCAIDI").alias("FCAIDI")
]).drop(["temp_assign", "temp_assign2"])

# PROC SUMMARY DATA=CAFY NWAY;
cafy_summary = cafy_df.group_by("REPTDATE").agg([
    pl.col("CURBAL1").sum().alias("TOTCAFY"),
    pl.col("ICURBAL1").sum().alias("TOTCAFYI"),
    pl.col("FCAIDC").sum().alias("TOFCAIDC"),
    pl.col("FCAIDI").sum().alias("TOFCAIDI"),
    pl.col("FCAFIC").sum().alias("TOFCAFIC"),
    pl.col("FCAFII").sum().alias("TOFCAFII")
])

print("CAFY Summary:")
print(cafy_summary)

# DATA FDFY;
# Define FCY list (you'll need to replace with actual values)
FCY = ["value1", "value2"]  # Replace with actual &FCY values

fdfy_df = pl.read_parquet(mnitb_path / "FD.parquet").filter(
    pl.col("PRODUCT").is_in(FCY)
).with_columns([
    pl.lit(int(RDATE)).alias("REPTDATE"),
    pl.col("CUSTCODE").map_dict(FDCUSTCD).alias("CUSTCD")
])

# DATA FCFY;
try:
    fcfy_df = pl.read_parquet(walk_path / f"WK{REPTYY}{REPTMON}{REPTDAY}.parquet").rename({
        "CURBAL": "FCURBAL"
    }).filter(
        pl.col("PROD") == "DCM22110"  # FOREIGN COMPANIES GL:22110
    ).with_columns([
        (pl.col("FCURBAL") * -1).alias("FCURBAL"),  # FCURBAL=(-1)*FCURBAL;
        pl.lit(int(RDATE)).alias("REPTDATE")
    ])
except FileNotFoundError:
    print(f"NOTE: WK{REPTYY}{REPTMON}{REPTDAY}.parquet not found")
    fcfy_df = pl.DataFrame(schema={"FCURBAL": pl.Float64, "REPTDATE": pl.Int64})

# Combine FDFY and FCFY
fdfy_combined = pl.concat([fdfy_df, fcfy_df]).with_columns([
    pl.lit(0.0).alias("CURBAL1"),
    pl.lit(0.0).alias("FFDFIC"),
    pl.lit(0.0).alias("FFDIDC")
]).with_columns([
    # IF CUSTCD IN (02,03,07,10,12,81,82,83,84) THEN DO;
    pl.when(
        pl.col("CUSTCD").is_in(["02", "03", "07", "10", "12", "81", "82", "83", "84"])
    ).then(pl.struct([
        pl.col("CURBAL").alias("FFDFIC")
    ])).otherwise(pl.struct([
        pl.col("CURBAL").alias("CURBAL1")
    ])).alias("temp_assign3")
]).with_columns([
    pl.col("temp_assign3").struct.field("FFDFIC").alias("FFDFIC"),
    pl.col("temp_assign3").struct.field("CURBAL1").alias("CURBAL1")
]).with_columns([
    # IF CUSTCODE IN (77,78,95,96) THEN FFDIDC = CURBAL;
    pl.when(
        pl.col("CUSTCODE").is_in([77, 78, 95, 96])
    ).then(pl.col("CURBAL")).otherwise(pl.col("FFDIDC")).alias("FFDIDC")
]).drop("temp_assign3")

# PROC SUMMARY DATA=FDFY NWAY;
fdfy_summary = fdfy_combined.group_by("REPTDATE").agg([
    pl.col("CURBAL1").sum().alias("TOTFDFY"),
    pl.col("FCURBAL").sum().alias("TOTFCFY"),
    pl.col("FFDFIC").sum().alias("TOFFDFIC"),
    pl.col("FFDIDC").sum().alias("TOFFDIDC")
])

print("FDFY Summary:")
print(fdfy_summary)

# DATA DYPOSN;
dyposn_df = cafy_summary.join(fdfy_summary, on="REPTDATE", how="outer")

print("DYPOSN merged data:")
print(dyposn_df)

# MACRO %PROCESS equivalent
def process_macro():
    if TDATE_STR != RDATE:
        print("DATA DATE IS NOT YESTERDAY- PLS RERUN LATER")
        sys.exit(77)
    else:
        output_file = mis_year_path / f"DYFCY{REPTMON}.parquet"
        
        if REPTDAY == "01":
            # First day of month - create new file
            dyposn_df.write_parquet(output_file)
            print(f"Created new file: {output_file}")
        else:
            # Append to existing file
            try:
                existing_df = pl.read_parquet(output_file)
                combined_df = pl.concat([dyposn_df, existing_df])
                combined_df.write_parquet(output_file)
                print(f"Appended to existing file: {output_file}")
            except FileNotFoundError:
                dyposn_df.write_parquet(output_file)
                print(f"Created new file (existing not found): {output_file}")
        
        # Read back the data for further processing
        final_df = pl.read_parquet(output_file)
        
        # Apply calculations and rounding
        final_df = final_df.with_columns([
            pl.col("TOTFDFY").round(0),
            pl.col("TOTFCFY").round(0),
            (pl.col("TOTFDFY") + pl.col("TOTFCFY")).alias("TOTFYFD"),
            pl.col("TOFFDFIC").round(0),  # COL 04
            pl.col("TOTCAFY").round(0),
            pl.col("TOTCAFYI").round(0),
            (pl.col("TOTCAFY") + pl.col("TOTCAFYI")).alias("TOTFYCA"),
            (pl.col("TOTFYFD") + pl.col("TOTFYCA")).alias("TOTFCY"),
            pl.col("TOFFDIDC").round(0),  # COL 09
            (pl.col("TOTFYFD") - pl.col("TOFFDIDC")).alias("TOFFDNDC"),  # COL 10
            pl.col("TOFCAFIC").round(0),  # COL 11
            pl.col("TOFCAFII").round(0),  # COL 12
            (pl.col("TOFCAFIC") + pl.col("TOFCAFII")).alias("TOTFCAFI"),  # COL 13
            (pl.col("TOFFDFIC") + pl.col("TOTFCAFI")).alias("TOTFCYFI"),  # COL 14
            pl.col("TOFCAIDC").round(0),  # COL 15
            (pl.col("TOTCAFY") - pl.col("TOFCAIDC")).alias("TOFCANDC"),  # COL 16
            pl.col("TOFCAIDI").round(0),  # COL 17
            (pl.col("TOTCAFYI") - pl.col("TOFCAIDI")).alias("TOFCANDI"),  # COL 18
            (pl.col("TOFCAIDC") + pl.col("TOFCAIDI")).alias("TOTFCAID"),  # COL 19
            (pl.col("TOFCANDC") + pl.col("TOFCANDI")).alias("TOTFCAND")  # COL 20
        ])
        
        # Remove duplicates by REPTDATE (equivalent to PROC SORT NODUPKEY)
        final_df = final_df.unique(subset=["REPTDATE"])
        
        # Save final result
        final_df.write_parquet(output_file)
        print(f"Final processed data saved to: {output_file}")
        
        # %INC PGM(DMMISR1F);
        try:
            import DMMISR1F
            DMMISR1F.process()
        except ImportError:
            print("NOTE: DMMISR1F.py not found")

# Execute the process
process_macro()

print("EIBDDEPF PROCESSING COMPLETED")
