from __future__ import annotations
from pathlib import Path
from datetime import datetime, date, timedelta
import polars as pl
import sys
import os
import re
import chardet

# ---- paths ----
GLFILE_TXT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/data/glfile_eivd.txt")
STORE_OUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIVDNLGL/")
STORE_OUT.mkdir(parents=True, exist_ok=True)

# ---- REPTDATE (yesterday) ----
REPTDATE = datetime.now() - timedelta(days=1)
REPTDATE = REPTDATE.date()
REPTYEAR = f"{REPTDATE.year:04d}"
REPTMON = f"{REPTDATE.month:02d}"
REPTDAY = f"{REPTDATE.day:02d}"
RDATE = f"{REPTDATE.day:02d}{REPTDATE.month:02d}{REPTDATE.year%100:02d}"

print("="*60)
print("EIVD GL PROCESSING STARTED (EIVDNLGL)")
print("="*60)
print(f"Processing date: {REPTDATE}")
print(f"Store directory: {STORE_OUT}")
print("="*60)

# ---- Helper function to detect encoding ----
def detect_encoding(filepath: Path) -> str:
    with open(filepath, 'rb') as f:
        raw_data = f.read(10000)
        result = chardet.detect(raw_data)
        return result['encoding']

# ---- Read GL text file with proper encoding ----
def read_gl_text_file(filepath: Path):
    """Read GL text file with proper encoding for EIVD version"""
    
    if not filepath.exists():
        raise FileNotFoundError(f"GL file not found: {filepath}")
    
    # Detect encoding
    encoding = detect_encoding(filepath)
    print(f"Detected encoding: {encoding}")
    
    # Try different encodings
    encodings_to_try = [encoding, 'utf-16', 'utf-16le', 'utf-16be', 'utf-8', 'latin-1', 'cp1252']
    if encoding not in encodings_to_try:
        encodings_to_try.insert(0, encoding)
    
    lines = None
    used_encoding = None
    
    for enc in encodings_to_try:
        try:
            with open(filepath, 'r', encoding=enc) as f:
                lines = [line.rstrip('\n') for line in f.readlines() if line.strip()]
            
            # Check if we got readable content
            if lines and len(lines) > 0:
                # Look for date pattern in first line
                first_line = lines[0].strip()
                if re.search(r'\d{8}', first_line):
                    used_encoding = enc
                    print(f"Successfully read file with encoding: {enc}")
                    break
                
                # Check for readable characters
                readable_count = sum(1 for c in first_line if c.isprintable() or c.isspace())
                if readable_count > len(first_line) * 0.3:
                    used_encoding = enc
                    print(f"Successfully read file with encoding: {enc}")
                    break
                    
        except Exception as e:
            print(f"Failed with encoding {enc}: {e}")
            continue
    
    if lines is None:
        raise RuntimeError("Unable to read file with any encoding")
    
    print(f"Total lines: {len(lines)}")
    
    # Show first few lines for debugging
    print("\nFirst 5 lines of data:")
    for i, line in enumerate(lines[:5]):
        print(f"Line {i+1}: '{line}'")
        # Try to extract using regex
        glitem_match = re.search(r'([A-Za-z0-9\- ]{3,10})', line)
        if glitem_match:
            print(f"  GLITEM: '{glitem_match.group(1)}'")
        date_match = re.search(r'(\d{2}/\d{2}/\d{2})', line)
        if date_match:
            print(f"  DATEX: '{date_match.group(1)}'")
        balance_match = re.search(r'([\d,]+\.?\d*)', line)
        if balance_match:
            print(f"  BALANCE: '{balance_match.group(1)}'")
        print()
    
    # Parse each line
    data = []
    header_date = None
    
    for line in lines:
        # Skip empty lines
        if not line.strip():
            continue
        
        # Remove null characters
        line = line.replace('\x00', '')
        
        # Try to find header date (8 digits)
        date_match = re.search(r'(\d{8})', line)
        if date_match and not header_date:
            # Check if this is the first line with just the date
            if len(line.strip()) <= 20:
                header_date = date_match.group(1)
                print(f"Header date found: {header_date}")
                continue
        
        # Try to find GLITEM (look for patterns like S-RCF, S-TLF, etc.)
        glitem_match = re.search(r'(1S-[A-Z\- ]{3,10})', line)
        if not glitem_match:
            # Try alternative pattern
            glitem_match = re.search(r'(S-[A-Z\- ]{3,10})', line)
        if not glitem_match:
            # Try to find any S- pattern
            glitem_match = re.search(r'(S-[A-Z]+)', line)
        
        if glitem_match:
            glitem = glitem_match.group(1).strip()
        else:
            # Skip lines without GLITEM
            continue
        
        # Skip if GLITEM is just 'S-' or empty
        if not glitem or len(glitem) < 3 or glitem == 'S-':
            continue
        
        # Try to find DATEX (DD/MM/YY)
        datex_match = re.search(r'(\d{2}/\d{2}/\d{2})', line)
        datex = datex_match.group(1) if datex_match else ''
        
        # Try to find BALANCE
        balance_match = re.search(r'([\d,]+\.?\d*)', line)
        balance_str = balance_match.group(1) if balance_match else ''
        
        # Check for sign at the end
        sign = ''
        if line.endswith('-'):
            sign = '-'
        
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
        
        # Convert DATEX from DD/MM/YY to DDMMYY
        if datex:
            datex_clean = datex.replace('/', '')
        else:
            datex_clean = header_date if header_date else RDATE
        
        # Only include valid data
        if glitem and balance != 0.0:  # Skip zero balances for cleaner data
            data.append({
                'GLITEM': glitem,
                'DATEX': datex_clean,
                'BALANCE': balance,
                'SIGN': sign
            })
    
    if not data:
        raise RuntimeError("No valid data parsed from GL file")
    
    df = pl.DataFrame(data)
    return df

# ---- Helper function to convert DDMMYY to date ----
def ddmmyy_to_date(s: str) -> date:
    """Convert DDMMYY string to date"""
    if not s or len(s) < 6:
        return REPTDATE
    # Remove any non-numeric characters
    s = re.sub(r'[^0-9]', '', s)
    if len(s) < 6:
        return REPTDATE
    try:
        dd = int(s[0:2])
        mm = int(s[2:4])
        yy2 = int(s[4:6])
        yy = 1900 + yy2 if yy2 >= 50 else 2000 + yy2
        return date(yy, mm, dd)
    except:
        return REPTDATE

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
for glitem in sorted(DF_G['GLITEM'].unique().to_list()):
    print(f"  '{glitem}'")

# ---- Get GL macro from first row ----
if "DATEX" in DF_G.columns:
    gl_ddmmyy = DF_G.select(pl.col("DATEX")).row(0)[0]
    GL = gl_ddmmyy
else:
    raise SystemExit("ABORT 77: GLFILE missing DATEX.")

print(f"\nGL Date from file: {GL}")
print(f"REPT Date: {RDATE}")

# ---- Main logic ----
if GL == RDATE:
    # Check required columns
    need = {"GLITEM", "DATEX", "BALANCE"}
    miss = need - set(DF_G.columns)
    if miss:
        raise SystemExit(f"ABORT 77: GLFILE missing {sorted(miss)}.")

    # ---- Process ----
    GLFILEP1 = (
        DF_G
        .with_columns([
            # Convert DATEX from DDMMYY to date
            pl.col("DATEX").map_elements(lambda s: ddmmyy_to_date(s), return_dtype=pl.Date).alias("DATE"),
        ])
        # ITEM + WEEK/MONTH
        .with_columns([
            pl.when(pl.col("GLITEM") == "1S-RCF").then(pl.lit("A1.35"))
             .when(pl.col("GLITEM") == "1S-GUARANTEE").then(pl.lit("A1.36"))
             .when(pl.col("GLITEM") == "1S-SM F").then(pl.lit("A1.37"))
             .when(pl.col("GLITEM").is_in(["1S-TLF", "1S-BA F"])).then(pl.lit("A1.38"))
             .when(pl.col("GLITEM").is_in(["1S-FIXED DEP", "1S-REMISIERFD"])).then(pl.lit("A2.01"))
             .otherwise(pl.lit(" ")).alias("ITEM"),
            pl.when(pl.col("GLITEM").is_in(["1S-RCF", "1S-GUARANTEE", "1S-SM F", "1S-TLF", "1S-BA F"]))
              .then(pl.col("BALANCE") * 0.2).otherwise(pl.lit(0.0)).alias("WEEK"),
            pl.when(pl.col("GLITEM").is_in(["1S-FIXED DEP", "1S-REMISIERFD"]))
              .then(pl.col("BALANCE")).otherwise(pl.lit(0.0)).alias("MONTH"),
            # Initialize other columns with 0
            pl.lit(0.0).alias("QTR"),
            pl.lit(0.0).alias("HALFYR"),
            pl.lit(0.0).alias("YEAR"),
            pl.lit(0.0).alias("LAST"),
            pl.lit(0.0).alias("TOTAL"),
        ])
        # BALANCE = SUM(WEEK, MONTH, QTR, HALFYR, YEAR, LAST, TOTAL)
        .with_columns(
            (pl.col("WEEK") + pl.col("MONTH") + pl.col("QTR") +
             pl.col("HALFYR") + pl.col("YEAR") + pl.col("LAST") + pl.col("TOTAL"))
            .alias("BALANCE_CALC")
        )
        .filter(pl.col("ITEM") != " ")
    )

    # Summary (NWAY sum by ITEM)
    SUMV = ["WEEK", "MONTH", "QTR", "HALFYR", "YEAR", "LAST", "BALANCE_CALC"]
    GL_SUM = GLFILEP1.group_by("ITEM").agg([pl.col(v).sum().alias(v) for v in SUMV])
    
    # Rename BALANCE_CALC to BALANCE
    GL_SUM = GL_SUM.rename({"BALANCE_CALC": "BALANCE"})

    # Rounding: ROUND(x, 1000.) / 1000
    R = GL_SUM.with_columns([
        ((pl.col(c) / 1000).round(0) / 1000).alias(c) for c in SUMV
    ])
    # Rename BALANCE_CALC back to BALANCE after rounding
    R = R.rename({"BALANCE_CALC": "BALANCE"})

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
