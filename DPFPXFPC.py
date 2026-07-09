from __future__ import annotations
from pathlib import Path
from datetime import datetime, date, timedelta
import polars as pl
import sys
import os

# ---- paths ----
GLFILE_TXT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/data/glfile_eivd.txt")  # EIVD GL text file
STORE_OUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIVDNLGL/")
STORE_OUT.mkdir(parents=True, exist_ok=True)

# ---- REPTDATE (yesterday) ----
REPTDATE = datetime.now() - timedelta(days=1)
REPTDATE = REPTDATE.date()
REPTYEAR = f"{REPTDATE.year:04d}"
REPTMON = f"{REPTDATE.month:02d}"
REPTDAY = f"{REPTDATE.day:02d}"
RDATE = f"{REPTDATE.day:02d}{REPTDATE.month:02d}{REPTDATE.year%100:02d}"  # DDMMYY

print("="*60)
print("EIVD GL PROCESSING STARTED (EIVDNLGL)")
print("="*60)
print(f"Processing date: {REPTDATE}")
print(f"Store directory: {STORE_OUT}")
print("="*60)

# ---- Read GL text file (fixed-width format) ----
def read_gl_text_file(filepath: Path):
    """Read GL text file with fixed-width format for EIVD version"""
    
    if not filepath.exists():
        raise FileNotFoundError(f"GL file not found: {filepath}")
    
    with open(filepath, 'r', encoding='ascii') as f:
        lines = [line.rstrip('\n') for line in f.readlines() if line.strip()]
    
    print(f"Total lines: {len(lines)}")
    
    # Show first few lines for debugging
    print("\nFirst 5 lines of data:")
    for i, line in enumerate(lines[:5]):
        print(f"Line {i+1}: '{line}'")
        if len(line) > 8:
            print(f"  Positions 0-8 (GLITEM): '{line[0:8].strip()}'")
        if len(line) > 28:
            print(f"  Positions 20-28 (DATEX): '{line[20:28].strip()}'")
        if len(line) > 60:
            print(f"  Positions 45-60 (BALANCE): '{line[45:60].strip()}'")
        print()
    
    # Parse each line
    data = []
    header_date = None
    
    for line in lines:
        # Skip empty lines
        if not line.strip():
            continue
        
        # Check if this is the header line (8 digits followed by spaces)
        stripped = line.strip()
        if stripped.isdigit() and len(stripped) == 8:
            header_date = stripped
            print(f"Header date found: {header_date}")
            continue
        
        # Extract GLITEM (positions 0-8)
        glitem = line[0:8].strip() if len(line) > 8 else ''
        
        # Skip if GLITEM is empty or just spaces
        if not glitem or glitem.isspace() or glitem == '08':
            continue
        
        # Extract DATEX (positions 20-28) - format DD/MM/YY
        datex = line[20:28].strip() if len(line) > 28 else ''
        
        # Extract BALANCE (positions 45-60)
        balance_str = line[45:60].strip() if len(line) > 60 else ''
        
        # Check for sign at the end
        sign = line[-1] if len(line) > 0 else ''
        
        # Clean balance string
        balance_str = balance_str.replace(',', '')
        
        # Convert to float
        try:
            balance = float(balance_str) if balance_str else 0.0
        except ValueError:
            balance = 0.0
        
        # Apply sign
        if sign == '-':
            balance = -balance
        
        # Convert DATEX from DD/MM/YY to DDMMYY format
        if datex:
            # datex is like "08/07/26"
            datex_clean = datex.replace('/', '')
        else:
            # Use header date if available
            if header_date:
                datex_clean = header_date
            else:
                # Use yesterday's date
                datex_clean = RDATE
        
        # Only include if GLITEM looks valid
        if glitem and len(glitem) >= 3:
            data.append({
                'GLITEM': glitem,
                'DATEX': datex_clean,  # DDMMYY format
                'BALANCE': balance,
                'SIGN': sign
            })
    
    if data:
        df = pl.DataFrame(data)
        return df
    else:
        raise RuntimeError("No data parsed from GL file")

# ---- Helper function to convert DDMMYY to date ----
def ddmmyy_to_date(s: str) -> date:
    """Convert DDMMYY string to date"""
    if len(s) != 6:
        # Try parsing with slashes
        s = s.replace('/', '')
    dd = int(s[0:2])
    mm = int(s[2:4])
    yy2 = int(s[4:6])
    yy = 1900 + yy2 if yy2 >= 50 else 2000 + yy2
    return date(yy, mm, dd)

# ---- Read GLFILE ----
try:
    DF_G = read_gl_text_file(GLFILE_TXT)
except Exception as e:
    print(f"ERROR reading file: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(77)

if DF_G.height == 0:
    raise SystemExit("ABORT 77: GLFILE is empty.")

print(f"\nParsed {DF_G.height} rows from GL file")
print(f"Columns: {DF_G.columns}")
print(f"\nData sample:")
print(DF_G.head(10))

# Show unique GLITEMs
print(f"\nUnique GLITEMs in file ({DF_G['GLITEM'].n_unique()}):")
for glitem in sorted(DF_G['GLITEM'].unique().to_list())[:20]:
    print(f"  '{glitem}'")
if DF_G['GLITEM'].n_unique() > 20:
    print(f"  ... and {DF_G['GLITEM'].n_unique() - 20} more")

# ---- Get GL macro from first row ----
if "DATEX" in DF_G.columns:
    gl_ddmmyy = DF_G.select(pl.col("DATEX")).row(0)[0]
    GL = gl_ddmmyy  # Already in DDMMYY format
else:
    raise SystemExit("ABORT 77: GLFILE missing DATEX (DDMMYY format).")

print(f"\nGL Date from file: {GL}")
print(f"REPT Date: {RDATE}")

# ---- Main logic ----
if GL == RDATE:
    # Check required columns
    need = {"GLITEM", "DATEX", "BALANCE", "SIGN"}
    miss = need - set(DF_G.columns)
    if miss:
        raise SystemExit(f"ABORT 77: GLFILE missing {sorted(miss)}.")

    # ---- Process ----
    GLFILEP1 = (
        DF_G
        .with_columns([
            # Convert DATEX from DDMMYY to date
            pl.col("DATEX").map_elements(lambda s: ddmmyy_to_date(s), return_dtype=pl.Date).alias("DATE"),
            # Apply sign
            pl.when(pl.col("SIGN") == "-")
              .then(-pl.col("BALANCE"))
              .otherwise(pl.col("BALANCE"))
              .alias("BALANCE"),
        ])
        # Flip positive BALANCE to negative for specific GLITEM values
        .with_columns(
            pl.when(
                pl.col("GLITEM").is_in(["S-RCF", "S-GUARANTEE", "S-SM F", "S-TLF", "S-BA F"])
            ).then(
                pl.when(pl.col("BALANCE") > 0, -pl.col("BALANCE")).otherwise(pl.col("BALANCE"))
            ).otherwise(pl.col("BALANCE"))
            .alias("BALANCE")
        )
        # ITEM + WEEK/MONTH
        .with_columns([
            pl.when(pl.col("GLITEM") == "S-RCF").then(pl.lit("A1.35"))
             .when(pl.col("GLITEM") == "S-GUARANTEE").then(pl.lit("A1.36"))
             .when(pl.col("GLITEM") == "S-SM F").then(pl.lit("A1.37"))
             .when(pl.col("GLITEM").is_in(["S-TLF", "S-BA F"])).then(pl.lit("A1.38"))
             .when(pl.col("GLITEM").is_in(["S-FIXED DEP", "S-REMISIERFD"])).then(pl.lit("A2.01"))
             .otherwise(pl.lit(" ")).alias("ITEM"),
            pl.when(pl.col("GLITEM").is_in(["S-RCF", "S-GUARANTEE", "S-SM F", "S-TLF", "S-BA F"]))
              .then(pl.col("BALANCE") * 0.2).otherwise(pl.lit(None)).alias("WEEK"),
            pl.when(pl.col("GLITEM").is_in(["S-FIXED DEP", "S-REMISIERFD"]))
              .then(pl.col("BALANCE")).otherwise(pl.lit(None)).alias("MONTH"),
            # Initialize other columns with 0
            pl.lit(0.0).alias("QTR"),
            pl.lit(0.0).alias("HALFYR"),
            pl.lit(0.0).alias("YEAR"),
            pl.lit(0.0).alias("LAST"),
            pl.lit(0.0).alias("TOTAL"),
        ])
        # BALANCE = SUM(WEEK, MONTH, QTR, HALFYR, YEAR, LAST, TOTAL)
        .with_columns(
            (pl.col("WEEK").fill_null(0.0) +
             pl.col("MONTH").fill_null(0.0) +
             pl.col("QTR").fill_null(0.0) +
             pl.col("HALFYR").fill_null(0.0) +
             pl.col("YEAR").fill_null(0.0) +
             pl.col("LAST").fill_null(0.0) +
             pl.col("TOTAL").fill_null(0.0))
            .alias("BALANCE")
        )
        .filter(pl.col("ITEM") != " ")
    )

    # Summary (NWAY sum by ITEM)
    SUMV = ["WEEK", "MONTH", "QTR", "HALFYR", "YEAR", "LAST", "BALANCE"]
    for v in SUMV:
        if v not in GLFILEP1.columns:
            GLFILEP1 = GLFILEP1.with_columns(pl.lit(0.0).alias(v))
        else:
            GLFILEP1 = GLFILEP1.with_columns(pl.col(v).fill_null(0.0).alias(v))

    GL_SUM = GLFILEP1.group_by("ITEM").agg([pl.col(v).sum().alias(v) for v in SUMV])

    # Rounding: ROUND(x, 1000.) / 1000
    R = GL_SUM.with_columns([
        ((pl.col(c) / 1000).round(0) / 1000).alias(c) for c in SUMV
    ])

    # Split & write
    def fname(stub: str) -> Path:
        return STORE_OUT / f"{stub}{REPTYEAR}{REPTMON}{REPTDAY}.parquet"

    print(f"\nCreating output files in: {STORE_OUT}")
    
    # Split by ITEM prefix
    A = (
        R.with_columns([
            pl.col("ITEM").str.slice(0, 1).alias("S1"),
            pl.col("ITEM").str.slice(1, 1).alias("S2"),
        ])
        .filter(pl.col("S1") == "A")
    )
    NA = (
        R.with_columns([
            pl.col("ITEM").str.slice(0, 1).alias("S1"),
            pl.col("ITEM").str.slice(1, 1).alias("S2"),
        ])
        .filter(pl.col("S1") != "A")
    )

    GLRMP1 = A.filter(pl.col("S2") == "1").drop(["S1", "S2"])
    GLUTRMP1 = A.filter(pl.col("S2") == "2").drop(["S1", "S2"])
    GLFXP1 = NA.filter(pl.col("S2") == "1").drop(["S1", "S2"])
    GLUTFXP1 = NA.filter(pl.col("S2") == "2").drop(["S1", "S2"])

    # Write parquet files
    if GLRMP1.height > 0:
        GLRMP1.write_parquet(fname("GLRMP1"))
        print(f"✓ Saved: GLRMP1{REPTYEAR}{REPTMON}{REPTDAY}.parquet")
        print(f"  Rows: {GLRMP1.height}")
        print(GLRMP1)
    
    if GLFXP1.height > 0:
        GLFXP1.write_parquet(fname("GLFXP1"))
        print(f"✓ Saved: GLFXP1{REPTYEAR}{REPTMON}{REPTDAY}.parquet")
        print(f"  Rows: {GLFXP1.height}")
        print(GLFXP1)
    
    if GLUTRMP1.height > 0:
        GLUTRMP1.write_parquet(fname("GLUTRMP1"))
        print(f"✓ Saved: GLUTRMP1{REPTYEAR}{REPTMON}{REPTDAY}.parquet")
        print(f"  Rows: {GLUTRMP1.height}")
        print(GLUTRMP1)
    
    if GLUTFXP1.height > 0:
        GLUTFXP1.write_parquet(fname("GLUTFXP1"))
        print(f"✓ Saved: GLUTFXP1{REPTYEAR}{REPTMON}{REPTDAY}.parquet")
        print(f"  Rows: {GLUTFXP1.height}")
        print(GLUTFXP1)

    print("\n" + "="*60)
    print("EIVD PROCESSING COMPLETE!")
    print("="*60)
    print(f"\nOutput files saved to: {STORE_OUT}")

    # List all created parquet files
    if STORE_OUT.exists():
        parquet_files = [f for f in STORE_OUT.iterdir() if f.suffix == '.parquet']
        if parquet_files:
            print(f"\n✓ {len(parquet_files)} parquet files created:")
            for f in sorted(parquet_files):
                file_size = f.stat().st_size
                print(f"  • {f.name} ({file_size:,} bytes)")

else:
    raise SystemExit(f"ABORT 77: THE GLFILE EXTRACTION IS NOT DATED {RDATE}")

print("\n" + "="*60)
