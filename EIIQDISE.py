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

mm1 = reptdate_val.month - 1
if mm1 == 0:
    mm1 = 12

now = datetime.now()

REPTMON = f"{reptdate_val.month:02d}"
PREVMON = f"{mm1:02d}"
NOWK = "4"
RPTDTE = now.strftime("%Y%m%d")
RPTTIME = now.strftime("%H%M%S")
CURDAY = f"{now.day:02d}"
CURMTH = f"{now.month:02d}"
CURYR = f"{now.year:04d}"
RPTYR = f"{reptdate_val.year:04d}"
RPTDY = f"{reptdate_val.day:02d}"

print(f"Month: {REPTMON}, Year: {RPTYR}")

# ==================== NPL DATA PROCESSING ====================
print("Processing NPL data...")
npl_df = pl.read_parquet(NPL_PATH / "IIS.parquet").unique(subset=["ACCTNO", "NOTENO"])
lnnote_df = pl.read_parquet(LOAN_PATH / "LNNOTE.parquet").select(["ACCTNO", "NOTENO", "COSTCTR"])

npl_df = npl_df.join(lnnote_df, on=["ACCTNO", "NOTENO"], how="left").with_columns([
    pl.when(pl.col("COSTCTR") == 1146)
    .then(146)
    .otherwise(pl.col("COSTCTR"))
]).sort("COSTCTR")

# ==================== CREATE GLPROC ====================
def create_glproc(df, is_pibb=False):
    if is_pibb:
        df = df.filter(
            ((3000 <= pl.col("COSTCTR")) & (pl.col("COSTCTR") <= 3999)) |
            (pl.col("COSTCTR").is_in([4043, 4048]))
        )
    
    results = []
    
    for row in df.iter_rows(named=True):
        is_hpd = row.get("LOANTYP", "").startswith("HPD CONV")
        
        # Define mappings based on loan type
        mappings = [
            ("SUSPEND", "ISHPDTE" if is_hpd else "ISAITAE"),
            ("RECOVER", "ISWHPDF" if is_hpd else "ISAITWF"),
            ("RECC", "ISHPDRG" if is_hpd else "ISAITRG"),
            ("OISUSP", "ISHPDTK" if is_hpd else "ISAITAK"),
            ("OIRECV", "ISWHPDL" if is_hpd else "ISAITWL"),
            ("OIRECC", "ISHPDRM" if is_hpd else "ISAITRM")
        ]
        
        # JDESC templates
        desc_templates = {
            "ISHPDTE": "HPD CONV INTEREST SUSPENDED DURING THE PERIOD (E)",
            "ISWHPDF": "HPD CONV WRITTEN BACK TO PROFIT & LOSS (F)",
            "ISHPDRG": "HPD CONV REVERSAL OF CURRENT YEAR IIS (G)",
            "ISHPDTK": "HPD CONV OI SUSPENDED DURING THE PERIOD (K)",
            "ISWHPDL": "HPD CONV WRITTEN BACK TO PROFIT & LOSS (L)",
            "ISHPDRM": "HPD CONV REVERSAL OF CURRENT YEAR IIS (M)",
            "ISAITAE": "HPD AITAB INTEREST SUSPENDED DURING THE PERIOD (E)",
            "ISAITWF": "HPD AITAB WRITTEN BACK TO PROFIT & LOSS (F)",
            "ISAITRG": "HPD AITAB REVERSAL OF CURRENT YEAR IIS (G)",
            "ISAITAK": "HPD AITAB OI SUSPENDED DURING THE PERIOD (K)",
            "ISAITWL": "HPD AITAB WRITTEN BACK TO PROFIT & LOSS (L)",
            "ISAITRM": "HPD AITAB REVERSAL OF CURRENT YEAR IIS (M)"
        }
        
        for field, arid in mappings:
            value = row.get(field, 0)
            if value and value != 0:
                results.append({
                    "COSTCTR": row["COSTCTR"],
                    "ARID": arid,
                    "JDESC": desc_templates[arid],
                    "AVALUE": float(value)
                })
    
    if results:
        result_df = pl.DataFrame(results)
        return result_df.group_by(["COSTCTR", "ARID", "JDESC"]).agg([
            pl.col("AVALUE").sum().alias("AVALUE")
        ]).sort(["COSTCTR", "ARID"])
    return pl.DataFrame()

# Create summaries
glproc = create_glproc(npl_df, is_pibb=False)
glproc2 = create_glproc(npl_df, is_pibb=True)

# ==================== GENERATE GLINTER FILE ====================
print("Generating GLINTER file...")

def format_gl_amount(amount):
    """Format amount as Z17.2 without decimal"""
    if amount < 0:
        sign = "-"
        amount = abs(amount)
    else:
        sign = "+"
    return sign, f"{amount:017.2f}".replace(".", "")

if len(glproc2) > 0:
    lines = []
    
    # Header record
    header = f"INTFISF20{RPTYR}{REPTMON}{RPTDY}{RPTDTE}{RPTTIME}"
    lines.append(header.ljust(40))
    
    # Process groups
    grouped = glproc2.group_by(["COSTCTR", "ARID", "JDESC"]).agg([
        pl.col("AVALUE").sum().alias("AVALUE")
    ]).sort(["COSTCTR", "ARID"])
    
    amttotc = 0.0
    amttotd = 0.0
    cnt = 0
    
    for row in grouped.iter_rows(named=True):
        avalue = row["AVALUE"]
        asign, formatted = format_gl_amount(avalue)
        
        if asign == "-":
            amttotc += abs(avalue)
        else:
            amttotd += avalue
        
        # Determine date fields
        if row["ARID"].endswith("COR"):
            date1 = f"{CURYR}{CURMTH}{CURDAY}"
            date2 = f"{CURYR}{CURMTH}{CURDAY}"
        else:
            date1 = f"{RPTYR}{REPTMON}{RPTDY}"
            date2 = f"{RPTYR}{REPTMON}{RPTDY}"
        
        cnt += 1
        detail = (f"INTFISF21{row['ARID']:<8}PIBB{row['COSTCTR']:04d}" + 
                  " " * 49 + "MYR" + 
                  " " * 3 + date1 + 
                  f"{asign}{formatted}{row['JDESC']:<60}{row['ARID']:<8}IISL" +
                  " " * 72 + "+" + "0" * 16 +
                  " " * 4 + "+" + "0" * 15 +
                  " " * 8 + date2 +
                  "+" + "0" * 16)
        lines.append(detail.ljust(324))
    
    # Format totals
    _, amttotco = format_gl_amount(amttotc)
    _, amttotdo = format_gl_amount(amttotd)
    
    # Footer record
    footer = (f"INTFISF29{RPTYR}{REPTMON}{RPTDY}{RPTDTE}{RPTTIME}" +
              f"+{amttotdo}-{amttotco}+{'0'*16}-{'0'*16}{cnt:09d}")
    lines.append(footer.ljust(117))
    
    # Write file
    gl_file = OUTPUT_PATH / f"GLINTER_QDISE_{RPTDTE}_{RPTTIME}.txt"
    with open(gl_file, 'w') as f:
        f.write('\n'.join(lines))
    
    print(f"GLINTER file created: {gl_file}")
    print(f"Records: {cnt}, Debit: {amttotd:,.2f}, Credit: {amttotc:,.2f}")

# ==================== SAVE AND REPORT ====================
if len(glproc) > 0:
    glproc.write_parquet(OUTPUT_PATH / f"GLPROC_QDISE_{REPTMON}_{RPTYR}.parquet")
if len(glproc2) > 0:
    glproc2.write_parquet(OUTPUT_PATH / f"GLPROC2_QDISE_{REPTMON}_{RPTYR}.parquet")

# Generate summary
report_file = OUTPUT_PATH / f"QDISE_SUMMARY_{REPTMON}_{RPTYR}.txt"
with open(report_file, 'w') as f:
    f.write(f"QDISE PROCESSING SUMMARY - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write("=" * 60 + "\n")
    f.write(f"Month: {REPTMON}, Previous: {PREVMON}, Year: {RPTYR}\n")
    f.write(f"NPL Records: {len(npl_df)}\n")
    f.write(f"GLPROC Records: {len(glproc)}\n")
    f.write(f"GLPROC2 (PIBB) Records: {len(glproc2)}\n")
    
    if len(glproc2) > 0:
        f.write("\nPIBB Summary by ARID:\n")
        for arid, group in glproc2.group_by("ARID"):
            total = group["AVALUE"].sum()
            f.write(f"  {arid}: {total:15,.2f}\n")

print("EIIQDISE processing complete!")
