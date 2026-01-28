import polars as pl
from datetime import datetime
from pathlib import Path

# ==================== SETUP ====================
BASE_PATH = Path("/path/to/data")
LOAN_PATH = BASE_PATH / "loan"
NPL_PATH = BASE_PATH / "npl"
OUTPUT_PATH = BASE_PATH / "output"

# ==================== REPTDATE PROCESSING ====================
print("Processing report date...")
reptdate_df = pl.read_parquet(LOAN_PATH / "REPTDATE.parquet")
reptdate_val = reptdate_df[0, "REPTDATE"]

# Calculate previous month
mm1 = reptdate_val.month - 1
if mm1 == 0:
    mm1 = 12

# Current date/time
now = datetime.now()
dt = now.date()
tm = now.time()

REPTMON = f"{reptdate_val.month:02d}"
PREVMON = f"{mm1:02d}"
RDATE = reptdate_val.strftime("%d-%B-%Y").upper()
REPTDAT = reptdate_val
NOWK = "4"
RPTDTE = now.strftime("%Y%m%d")
RPTTIME = now.strftime("%H%M%S")
CURDAY = f"{now.day:02d}"
CURMTH = f"{now.month:02d}"
CURYR = f"{now.year:04d}"
RPTYR = f"{reptdate_val.year:04d}"
RPTDY = f"{reptdate_val.day:02d}"

print(f"Report Date: {RDATE}, Month: {REPTMON}")

# ==================== NPL DATA PROCESSING ====================
print("Processing NPL data...")
# Read and deduplicate NPL data
npl_df = pl.read_parquet(NPL_PATH / "IIS.parquet").unique(subset=["ACCTNO", "NOTENO"])

# Read LNNOTE data
lnnote_df = pl.read_parquet(LOAN_PATH / "LNNOTE.parquet").select(["ACCTNO", "NOTENO", "COSTCTR"])

# Merge and clean COSTCTR
npl_df = npl_df.join(lnnote_df, on=["ACCTNO", "NOTENO"], how="left")
npl_df = npl_df.with_columns([
    pl.when(pl.col("COSTCTR") == 1146)
    .then(146)
    .otherwise(pl.col("COSTCTR"))
    .alias("COSTCTR")
]).sort("COSTCTR")

# ==================== GLPROC PROCESSING ====================
def create_glproc(df, is_pibb=False):
    """Create GLPROC dataset based on loan type and conditions"""
    
    # Filter for PIBB if needed
    if is_pibb:
        df = df.filter(
            ((3000 <= pl.col("COSTCTR")) & (pl.col("COSTCTR") <= 3999)) |
            (pl.col("COSTCTR").is_in([4043, 4048]))
        )
    
    # Initialize empty list for results
    results = []
    
    # Define ARID mappings for different loan types and amounts
    arid_mappings = [
        # HPD CONV loan type
        {
            "condition": lambda row: row["LOANTYP"][:8] == "HPD CONV",
            "fields": [
                ("SUSPEND", "ISHPDTE", "HPD CONV INTEREST SUSPENDED DURING THE PERIOD (E)"),
                ("RECOVER", "ISWHPDF", "HPD CONV WRITTEN BACK TO PROFIT & LOSS (F)"),
                ("RECC", "ISHPDRG", "HPD CONV REVERSAL OF CURRENT YEAR IIS (G)"),
                ("OISUSP", "ISHPDTK", "HPD CONV OI SUSPENDED DURING THE PERIOD (K)"),
                ("OIRECV", "ISWHPDL", "HPD CONV WRITTEN BACK TO PROFIT & LOSS (L)"),
                ("OIRECC", "ISHPDRM", "HPD CONV REVERSAL OF CURRENT YEAR IIS (M)")
            ]
        },
        # Other loan types
        {
            "condition": lambda row: row["LOANTYP"][:8] != "HPD CONV",
            "fields": [
                ("SUSPEND", "ISAITAE", "HPD AITAB INTEREST SUSPENDED DURING THE PERIOD (E)"),
                ("RECOVER", "ISAITWF", "HPD AITAB WRITTEN BACK TO PROFIT & LOSS (F)"),
                ("RECC", "ISAITRG", "HPD AITAB REVERSAL OF CURRENT YEAR IIS (G)"),
                ("OISUSP", "ISAITAK", "HPD AITAB OI SUSPENDED DURING THE PERIOD (K)"),
                ("OIRECV", "ISAITWL", "HPD AITAB WRITTEN BACK TO PROFIT & LOSS (L)"),
                ("OIRECC", "ISAITRM", "HPD AITAB REVERSAL OF CURRENT YEAR IIS (M)")
            ]
        }
    ]
    
    # Process each row
    for row in df.iter_rows(named=True):
        # Find the appropriate mapping based on loan type
        for mapping in arid_mappings:
            if mapping["condition"](row):
                for field_name, arid, jdesc in mapping["fields"]:
                    value = row.get(field_name, 0)
                    if value not in (0, None):
                        results.append({
                            "COSTCTR": row["COSTCTR"],
                            "ARID": arid,
                            "JDESC": jdesc,
                            "AVALUE": float(value)
                        })
                break  # Found the matching mapping
    
    # Create DataFrame and summarize
    if results:
        result_df = pl.DataFrame(results)
        # Group by COSTCTR, ARID, JDESC and sum AVALUE
        summary = result_df.group_by(["COSTCTR", "ARID", "JDESC"]).agg([
            pl.col("AVALUE").sum().alias("AVALUE")
        ]).sort(["COSTCTR", "ARID"])
        return summary
    return pl.DataFrame()

# Create GLPROC (all data)
print("Creating GLPROC summary...")
glproc = create_glproc(npl_df, is_pibb=False)

# Create GLPROC2 (PIBB only)
print("Creating GLPROC2 summary (PIBB)...")
glproc2 = create_glproc(npl_df, is_pibb=True)

# ==================== GLINTER FILE GENERATION ====================
print("Generating GLINTER file...")

def format_amount(value):
    """Format amount as Z17.2 without decimal point"""
    if value < 0:
        sign = "-"
        value = abs(value)
    else:
        sign = "+"
    
    # Format as string with 2 decimal places, remove decimal point
    formatted = f"{value:017.2f}".replace(".", "")
    return sign, formatted

def write_gl_file(glproc2_df, output_path):
    """Write GLINTER file with fixed-width format"""
    
    if len(glproc2_df) == 0:
        print("No PIBB data to process")
        return
    
    # Group by ARID and calculate totals
    grouped = glproc2_df.group_by(["COSTCTR", "ARID", "JDESC"]).agg([
        pl.col("AVALUE").sum().alias("AVALUE")
    ]).sort(["COSTCTR", "ARID"])
    
    lines = []
    
    # Header record (record type 0)
    header = f"INTFISF20{RPTYR}{REPTMON}{RPTDY}{RPTDTE}{RPTTIME}"
    lines.append(header.ljust(40))
    
    # Initialize totals
    amttotc = 0.0  # Credit total
    amttotd = 0.0  # Debit total
    cnt = 0
    
    # Process each ARID group
    for row in grouped.iter_rows(named=True):
        avalue = row["AVALUE"]
        arid = row["ARID"]
        costctr = row["COSTCTR"]
        jdesc = row["JDESC"]
        
        # Determine sign and format amount
        if avalue < 0:
            asign = "-"
            avalue_abs = abs(avalue)
            amttotc += avalue_abs
        else:
            asign = "+"
            avalue_abs = avalue
            amttotd += avalue_abs
        
        # Format amount
        _, brhamto = format_amount(avalue_abs)
        
        # Determine date fields based on ARID
        if arid.endswith("COR"):
            date_field1 = f"{CURYR}{CURMTH}{CURDAY}"
            date_field2 = f"{CURYR}{CURMTH}{CURDAY}"
        else:
            date_field1 = f"{RPTYR}{REPTMON}{RPTDY}"
            date_field2 = f"{RPTYR}{REPTMON}{RPTDY}"
        
        # Create detail record (record type 1)
        cnt += 1
        detail = (f"INTFISF21{arid:<8}PIBB{costctr:04d}" + 
                  " " * (71-22) + "MYR" + 
                  " " * (77-74) + date_field1 + 
                  f"{asign}{brhamto}{jdesc:<60}{arid:<8}IISL" +
                  " " * (252-172) + "+" + "0" * 16 +
                  " " * (273-269) + "+" + "0" * 15 +
                  " " * (297-289) + date_field2 +
                  "+" + "0" * 16)
        lines.append(detail.ljust(324))
    
    # Format totals
    _, amttotco = format_amount(amttotc)
    _, amttotdo = format_amount(amttotd)
    
    # Footer record (record type 9)
    footer = (f"INTFISF29{RPTYR}{REPTMON}{RPTDY}{RPTDTE}{RPTTIME}" +
              f"+{amttotdo}-{amttotco}+{'0'*16}-{'0'*16}{cnt:09d}")
    lines.append(footer.ljust(117))
    
    # Write to file
    gl_file = output_path / f"GLINTER_{RPTDTE}_{RPTTIME}.txt"
    with open(gl_file, 'w') as f:
        for line in lines:
            f.write(line + '\n')
    
    print(f"GLINTER file generated: {gl_file}")
    print(f"Records processed: {cnt}")
    print(f"Debit total: {amttotd:,.2f}")
    print(f"Credit total: {amttotc:,.2f}")

# Generate GLINTER file
write_gl_file(glproc2, OUTPUT_PATH)

# ==================== SAVE SUMMARIES ====================
print("Saving summary files...")

if len(glproc) > 0:
    glproc.write_parquet(OUTPUT_PATH / f"GLPROC_{REPTMON}_{RPTYR}.parquet")
    print(f"GLPROC saved: {len(glproc)} records")

if len(glproc2) > 0:
    glproc2.write_parquet(OUTPUT_PATH / f"GLPROC2_{REPTMON}_{RPTYR}.parquet")
    print(f"GLPROC2 (PIBB) saved: {len(glproc2)} records")

# ==================== SUMMARY REPORT ====================
print("Generating summary report...")
report_file = OUTPUT_PATH / f"IIS_SUMMARY_{REPTMON}_{RPTYR}.txt"

with open(report_file, 'w') as f:
    f.write("INTEREST IN SUSPENSE (IIS) PROCESSING SUMMARY\n")
    f.write("=" * 80 + "\n")
    f.write(f"Report Date: {RDATE}\n")
    f.write(f"Processing Date: {now.strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write(f"Month: {REPTMON}, Previous Month: {PREVMON}\n")
    f.write("-" * 80 + "\n")
    
    f.write("\nGLPROC SUMMARY:\n")
    if len(glproc) > 0:
        # Summarize by ARID
        arid_summary = glproc.group_by("ARID").agg([
            pl.col("AVALUE").sum().alias("TOTAL_VALUE"),
            pl.col("COSTCTR").n_unique().alias("UNIQUE_COSTCTR")
        ])
        
        for row in arid_summary.iter_rows(named=True):
            f.write(f"{row['ARID']:<10} {row['TOTAL_VALUE']:>15,.2f} ({row['UNIQUE_COSTCTR']} cost centers)\n")
    else:
        f.write("No data\n")
    
    f.write("\nGLPROC2 (PIBB) SUMMARY:\n")
    if len(glproc2) > 0:
        # Summarize by ARID
        arid_summary2 = glproc2.group_by("ARID").agg([
            pl.col("AVALUE").sum().alias("TOTAL_VALUE"),
            pl.col("COSTCTR").n_unique().alias("UNIQUE_COSTCTR")
        ])
        
        for row in arid_summary2.iter_rows(named=True):
            f.write(f"{row['ARID']:<10} {row['TOTAL_VALUE']:>15,.2f} ({row['UNIQUE_COSTCTR']} cost centers)\n")
    else:
        f.write("No data\n")
    
    f.write("\n" + "=" * 80 + "\n")
    f.write(f"Total NPL records processed: {len(npl_df)}\n")
    f.write(f"GLPROC records: {len(glproc)}\n")
    f.write(f"GLPROC2 (PIBB) records: {len(glproc2)}\n")

print("Processing complete!")
print(f"Summary report: {report_file}")
