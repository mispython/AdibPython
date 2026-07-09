from __future__ import annotations
from pathlib import Path
from datetime import datetime, date, timedelta
import polars as pl
import sys
import os
import re
import chardet

# ----------------------------
# Simple Paths - Islamic Version
# ----------------------------
GLFILE_TXT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/data/glfile_islamic.txt")
STORE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDNLGL/")
STORE_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------
# Helpers
# ----------------------------
def _fmt_DDMMYY8(d: date) -> str:
    return d.strftime("%d/%m/%y")

def _parse_DDMMYY8(s: str) -> date:
    return datetime.strptime(s, "%d/%m/%y").date()

def _round_thousands_div_thousand(expr: pl.Expr) -> pl.Expr:
    return (expr / 1000).round(0)

def _write_store(df: pl.DataFrame, name: str) -> None:
    out = STORE_DIR / f"{name}.parquet"
    df.write_parquet(out)
    print(f"✓ Saved: {out}")

def detect_encoding(filepath: Path) -> str:
    """Detect the encoding of the file"""
    with open(filepath, 'rb') as f:
        raw_data = f.read(10000)
        result = chardet.detect(raw_data)
        return result['encoding']

def read_gl_text_file_with_encoding(filepath: Path):
    """Read GL text file with proper encoding detection"""
    
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
                # Check if first line has the date pattern (digits)
                first_line = lines[0].strip()
                # Look for 8 digits in the first line
                if re.search(r'\d{8}', first_line):
                    used_encoding = enc
                    print(f"Successfully read file with encoding: {enc}")
                    break
                
                # Check if we have readable characters (not all control chars)
                readable_count = sum(1 for c in first_line if c.isprintable() or c.isspace())
                if readable_count > len(first_line) * 0.3:
                    used_encoding = enc
                    print(f"Successfully read file with encoding: {enc}")
                    break
                    
        except Exception as e:
            print(f"Failed with encoding {enc}: {e}")
            continue
    
    if lines is None:
        # Try reading as binary and extracting numbers
        print("All encodings failed, trying binary extraction...")
        with open(filepath, 'rb') as f:
            raw_data = f.read()
        
        # Extract all numbers and dates from binary
        # Look for DD/MM/YY pattern
        date_pattern = rb'\d{2}/\d{2}/\d{2}'
        dates = re.findall(date_pattern, raw_data)
        
        # Look for balance numbers with commas
        balance_pattern = rb'[\d,]+\.?\d*'
        balances = re.findall(balance_pattern, raw_data)
        
        print(f"Found {len(dates)} dates and {len(balances)} balances in binary")
        
        # Try to reconstruct data from binary
        # This is a fallback - create a simple structure
        if dates and balances:
            # Use the first date as header
            header_date = dates[0].decode('ascii') if dates else None
            # Use subsequent dates and balances for data
            # This is simplified - you may need to adjust based on your data
            print("Binary extraction found data, but manual parsing may be needed")
            raise RuntimeError("Unable to parse file with any encoding. Please check file format.")
        else:
            raise RuntimeError("No data found in file")
    
    print(f"\nTotal lines: {len(lines)}")
    
    # Show first few lines for debugging
    print("\nFirst 5 lines of data:")
    for i, line in enumerate(lines[:5]):
        print(f"Line {i+1}: '{line}'")
        print(f"  Length: {len(line)}")
        # Show hex for debugging
        if len(line) > 0:
            hex_chars = ' '.join(f'{ord(c):02x}' for c in line[:20])
            print(f"  Hex (first 20 chars): {hex_chars}")
        print()
    
    # Parse each line
    data = []
    header_date = None
    
    for line in lines:
        # Skip empty lines
        if not line.strip():
            continue
        
        # Try to find the header line (8 digits)
        # Look for 8 consecutive digits
        date_match = re.search(r'(\d{8})', line)
        if date_match and not header_date:
            header_date = date_match.group(1)
            print(f"Header date found: {header_date}")
            continue
        
        # Extract GLITEM (positions 0-8 if available, otherwise try to find pattern)
        if len(line) >= 8:
            glitem = line[0:8].strip()
        else:
            glitem = line.strip()
        
        # Skip if GLITEM is empty or just spaces
        if not glitem or glitem.isspace() or glitem in ['', '†', 'ഊ', '††††††††']:
            continue
        
        # Try to find date pattern (DD/MM/YY)
        date_match = re.search(r'(\d{2}/\d{2}/\d{2})', line)
        date_str = date_match.group(1) if date_match else ''
        
        # Try to find balance pattern (numbers with commas and decimals)
        balance_match = re.search(r'([\d,]+\.?\d*)', line)
        balance_str = balance_match.group(1) if balance_match else ''
        
        # Check for sign at the end
        sign = ''
        if line.endswith('-') or '- ' in line:
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
        
        # Get date from header
        if header_date:
            yy = header_date[0:2]
            mm = header_date[2:4]
            dd = header_date[4:6]
            gl_date = date(int(f"20{yy}"), int(mm), int(dd))
        else:
            yesterday = datetime.now() - timedelta(days=1)
            gl_date = yesterday.date()
        
        # Parse the date from the file if available
        if date_str:
            try:
                datex = _parse_DDMMYY8(date_str)
            except:
                datex = gl_date
        else:
            datex = gl_date
        
        # Only include if GLITEM looks valid (not just a single digit)
        if glitem and len(glitem) >= 3 and glitem not in ['08', '†']:
            data.append({
                'GLITEM': glitem,
                'DATEX': gl_date.strftime("%d/%m/%y"),
                'BALANCE': balance,
                'SIGN': sign,
                'DATE': datex
            })
    
    if data:
        df = pl.DataFrame(data)
        return df
    else:
        raise RuntimeError("No data parsed from GL file")

def match_glitem(file_glitem, condition_glitem):
    """Try to match file GLITEM with condition GLITEM using flexible matching"""
    if not file_glitem or not condition_glitem:
        return False
    
    file_clean = file_glitem.strip()
    cond_clean = condition_glitem.strip()
    
    # Direct match
    if file_clean == cond_clean:
        return True
    
    # Check if one contains the other
    if file_clean in cond_clean or cond_clean in file_clean:
        return True
    
    # Handle '1F' prefix variations
    if file_clean.startswith('1F') and cond_clean.startswith('F'):
        file_no_prefix = file_clean[2:]
        cond_no_prefix = cond_clean[1:]
        if file_no_prefix == cond_no_prefix:
            return True
        if file_no_prefix in cond_no_prefix or cond_no_prefix in file_no_prefix:
            return True
    
    if file_clean.startswith('1F'):
        file_no_prefix = file_clean[2:]
        if file_no_prefix == cond_clean:
            return True
        if file_no_prefix in cond_clean or cond_clean in file_no_prefix:
            return True
    
    # Handle missing prefix
    if cond_clean.startswith('F') and not file_clean.startswith('F'):
        cond_no_f = cond_clean[1:]
        if file_clean == cond_no_f:
            return True
        if file_clean in cond_no_f or cond_no_f in file_clean:
            return True
    
    # Check last 5 characters
    if len(file_clean) >= 5 and len(cond_clean) >= 5:
        if file_clean[-5:] == cond_clean[-5:]:
            return True
    
    return False

# ----------------------------
# 1) Get REPTDATE (yesterday for Islamic version)
# ----------------------------
REPTDATE = datetime.now() - timedelta(days=1)
REPTDATE = REPTDATE.date()
REPTYEAR = REPTDATE.strftime("%Y")
REPTMON = REPTDATE.strftime("%m")
REPTDAY = REPTDATE.strftime("%d")
RDATE = _fmt_DDMMYY8(REPTDATE)

print("="*60)
print("ISLAMIC GL PROCESSING STARTED (EIIDNLGL)")
print("="*60)
print(f"Processing date: {REPTDATE}")
print(f"Store directory: {STORE_DIR}")
print("="*60)

# ----------------------------
# 2) Read GLFILE with proper encoding
# ----------------------------
try:
    gl_df_raw = read_gl_text_file_with_encoding(GLFILE_TXT)
except Exception as e:
    print(f"ERROR reading file: {e}")
    sys.exit(77)

if gl_df_raw.height == 0:
    raise RuntimeError("GLFILE is empty.")

print(f"\nParsed {gl_df_raw.height} rows from GL file")
print(f"Columns: {gl_df_raw.columns}")
print(f"\nData sample:")
print(gl_df_raw.head(10))

# Show unique GLITEMs
print(f"\nUnique GLITEMs in file ({gl_df_raw['GLITEM'].n_unique()}):")
for glitem in sorted(gl_df_raw['GLITEM'].unique().to_list()):
    print(f"  '{glitem}'")

# Get GL date from first record's DATEX
if gl_df_raw.height > 0:
    try:
        GLDATE = _parse_DDMMYY8(gl_df_raw.select("DATEX").row(0)[0])
        GL = _fmt_DDMMYY8(GLDATE)
        print(f"\nGL Date from file: {GL}")
    except:
        print(f"\nGL Date from file: Using REPTDATE")
        GL = RDATE
else:
    GL = RDATE

print(f"REPT Date: {RDATE}")

# ----------------------------
# %MACRO PROCESS gate: proceed only if GL == RDATE; else ABORT 77
# ----------------------------
if GL != RDATE:
    print(f"THE GLFILE EXTRACTION IS NOT DATED {RDATE}")
    sys.exit(77)

# ----------------------------
# Common transformation for both passes (base columns)
# ----------------------------
def _prep_base(gl: pl.DataFrame) -> pl.DataFrame:
    return (
        gl.with_columns(
            DATE = pl.col("DATEX").str.to_datetime(format="%d/%m/%y").dt.date(),
            BALANCE = pl.when(pl.col("SIGN") == "-")
                        .then(pl.col("BALANCE") * -1)
                        .otherwise(pl.col("BALANCE")),
            WEEK    = pl.lit(0.0),
            MONTH   = pl.lit(0.0),
            QTR     = pl.lit(0.0),
            HALFYR  = pl.lit(0.0),
            YEAR    = pl.lit(0.0),
            LAST    = pl.lit(0.0),
            TOTAL   = pl.lit(0.0),
            ITEM    = pl.lit(""),
        )
    )

def _apply_mapping_pass1(df: pl.DataFrame) -> pl.DataFrame:
    # P1 mapping (A2.21)
    return (
        df.with_columns(
            ITEM = pl.when(pl.col("GLITEM").is_in(["49120", "49120NLF"]))
                       .then(pl.lit("A1.20"))
                 .when(pl.col("GLITEM").is_in(["F143120ODNCB", "F143120ODNIB"]))
                       .then(pl.lit("A2.21"))
                 .when(pl.col("GLITEM").is_in(["F13312002CB", "F132121BBNM"]))
                       .then(pl.lit("A2.01"))
                 .when(pl.col("GLITEM") == "37070")
                       .then(pl.lit("A2.08"))
                 .otherwise(pl.lit("")),
            WEEK = pl.when(
                        pl.col("GLITEM").is_in(
                            ["49120","49120NLF","F143120ODNCB","F143120ODNIB",
                             "F13312002CB","F132121BBNM","37070"]
                        )
                    ).then(pl.col("BALANCE"))
                     .otherwise(pl.col("WEEK"))
        )
        .with_columns(
            BALANCE = pl.col("WEEK") + pl.col("MONTH") + pl.col("QTR") +
                      pl.col("HALFYR") + pl.col("YEAR") + pl.col("LAST") + pl.col("TOTAL")
        )
        .filter(pl.col("ITEM") != "")
    )

def _apply_mapping_pass2(df: pl.DataFrame) -> pl.DataFrame:
    # P2 mapping (A2.14)
    return (
        df.with_columns(
            ITEM = pl.when(pl.col("GLITEM").is_in(["49120", "49120NLF"]))
                       .then(pl.lit("A1.20"))
                 .when(pl.col("GLITEM").is_in(["F143120ODNCB", "F143120ODNIB"]))
                       .then(pl.lit("A2.14"))
                 .when(pl.col("GLITEM").is_in(["F13312002CB", "F132121BBNM"]))
                       .then(pl.lit("A2.01"))
                 .when(pl.col("GLITEM") == "37070")
                       .then(pl.lit("A2.08"))
                 .otherwise(pl.lit("")),
            WEEK = pl.when(
                        pl.col("GLITEM").is_in(
                            ["49120","49120NLF","F143120ODNCB","F143120ODNIB",
                             "F13312002CB","F132121BBNM","37070"]
                        )
                    ).then(pl.col("BALANCE"))
                     .otherwise(pl.col("WEEK"))
        )
        .with_columns(
            BALANCE = pl.col("WEEK") + pl.col("MONTH") + pl.col("QTR") +
                      pl.col("HALFYR") + pl.col("YEAR") + pl.col("LAST") + pl.col("TOTAL")
        )
        .filter(pl.col("ITEM") != "")
    )

def _summary_by_item(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.group_by("ITEM")
          .agg([
              pl.col("WEEK").sum().alias("WEEK"),
              pl.col("MONTH").sum().alias("MONTH"),
              pl.col("QTR").sum().alias("QTR"),
              pl.col("HALFYR").sum().alias("HALFYR"),
              pl.col("YEAR").sum().alias("YEAR"),
              pl.col("LAST").sum().alias("LAST"),
              pl.col("BALANCE").sum().alias("BALANCE"),
          ])
          .sort("ITEM")
    )

def _apply_rounding_and_split(df: pl.DataFrame, pass_label: str) -> None:
    rounded = (
        df.with_columns(
            WEEK    = _round_thousands_div_thousand(pl.col("WEEK")),
            MONTH   = _round_thousands_div_thousand(pl.col("MONTH")),
            QTR     = _round_thousands_div_thousand(pl.col("QTR")),
            HALFYR  = _round_thousands_div_thousand(pl.col("HALFYR")),
            YEAR    = _round_thousands_div_thousand(pl.col("YEAR")),
            LAST    = _round_thousands_div_thousand(pl.col("LAST")),
            BALANCE = _round_thousands_div_thousand(pl.col("BALANCE")),
        )
    )

    first1 = rounded["ITEM"].str.slice(0, 1)
    second1 = rounded["ITEM"].str.slice(1, 1)

    A_mask   = first1 == "A"
    notA     = first1 != "A"
    sec_is_1 = second1 == "1"
    sec_is_2 = second1 == "2"

    YYYYMMDD = f"{REPTYEAR}{REPTMON}{REPTDAY}"

    GLRMP  = rounded.filter(A_mask & sec_is_1)
    GLUTRM = rounded.filter(A_mask & sec_is_2)
    GLFXP  = rounded.filter(notA  & sec_is_1)
    GLUTFX = rounded.filter(notA  & sec_is_2)

    print(f"\nSaving Islamic version outputs for pass {pass_label}:")
    
    if GLRMP.height > 0:
        _write_store(GLRMP,  f"GLRMP{pass_label}{YYYYMMDD}")
        print(f"  GLRMP{pass_label}{YYYYMMDD}: {GLRMP.height} rows")
        print(GLRMP)
    
    if GLFXP.height > 0:
        _write_store(GLFXP,  f"GLFXP{pass_label}{YYYYMMDD}")
        print(f"  GLFXP{pass_label}{YYYYMMDD}: {GLFXP.height} rows")
        print(GLFXP)
    
    if GLUTRM.height > 0:
        _write_store(GLUTRM, f"GLUTRMP{pass_label}{YYYYMMDD}")
        print(f"  GLUTRMP{pass_label}{YYYYMMDD}: {GLUTRM.height} rows")
        print(GLUTRM)
    
    if GLUTFX.height > 0:
        _write_store(GLUTFX, f"GLUTFXP{pass_label}{YYYYMMDD}")
        print(f"  GLUTFXP{pass_label}{YYYYMMDD}: {GLUTFX.height} rows")
        print(GLUTFX)

# ----------------------------
# Build both passes
# ----------------------------
print("\n" + "="*60)
print("Preparing base data...")
print("="*60)
base = _prep_base(gl_df_raw)
print(f"Base data shape: {base.shape}")

# Show what we're trying to match
print("\nLooking for these GLITEMs in the file:")
p1_mappings = ["49120", "49120NLF", "F143120ODNCB", "F143120ODNIB", 
               "F13312002CB", "F132121BBNM", "37070"]
for glitem in p1_mappings:
    print(f"  {glitem}")

# Try to match using flexible matching
print("\nAttempting flexible matching...")
file_glitems = base['GLITEM'].unique().to_list()
matching = {}
for file_glitem in file_glitems:
    for cond_glitem in p1_mappings:
        if match_glitem(file_glitem, cond_glitem):
            matching[file_glitem] = cond_glitem
            print(f"  Matched: '{file_glitem}' -> '{cond_glitem}'")
            break

if matching:
    print(f"\nFound {len(matching)} matches")
    # Update the base with matched items
    for file_glitem, cond_glitem in matching.items():
        base = base.with_columns(
            pl.when(pl.col("GLITEM") == file_glitem)
              .then(pl.lit(cond_glitem))
              .otherwise(pl.col("GLITEM"))
              .alias("GLITEM")
        )
else:
    print("\nNo matches found. Please check the GLITEM values in your file.")
    print("Available GLITEMs in file:")
    for glitem in sorted(file_glitems):
        print(f"  '{glitem}'")

# Pass 1
print("\n" + "="*60)
print("Processing Islamic GL P1...")
print("="*60)

p1 = _apply_mapping_pass1(base)
print(f"P1 mapped shape: {p1.shape}")

if p1.height > 0:
    p1_sum = _summary_by_item(p1)
    print(f"P1 summary: {p1_sum.shape}")
    print("\nP1 Summary Data:")
    print(p1_sum)
    _apply_rounding_and_split(p1_sum, pass_label="1")
else:
    print("No data for P1")

# Pass 2
print("\n" + "="*60)
print("Processing Islamic GL P2...")
print("="*60)

p2 = _apply_mapping_pass2(base)
print(f"P2 mapped shape: {p2.shape}")

if p2.height > 0:
    p2_sum = _summary_by_item(p2)
    print(f"P2 summary: {p2_sum.shape}")
    print("\nP2 Summary Data:")
    print(p2_sum)
    _apply_rounding_and_split(p2_sum, pass_label="2")
else:
    print("No data for P2")

# ----------------------------
# Summary
# ----------------------------
print("\n" + "="*60)
print("ISLAMIC PROCESSING COMPLETE!")
print("="*60)
print(f"\nOutput files saved to: {STORE_DIR}")

if STORE_DIR.exists():
    parquet_files = [f for f in STORE_DIR.iterdir() if f.suffix == '.parquet']
    if parquet_files:
        print(f"\n✓ {len(parquet_files)} parquet files created:")
        for f in sorted(parquet_files):
            file_size = f.stat().st_size
            print(f"  • {f.name} ({file_size:,} bytes)")
    else:
        print("\n⚠ No parquet files found in the output directory.")

print("\n" + "="*60)
