from __future__ import annotations
from pathlib import Path
from datetime import datetime, date, timedelta
import polars as pl
import sys
import os
import re

# ----------------------------
# Simple Paths - Islamic Version
# ----------------------------
GLFILE_TXT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/data/glfile_islamic.txt")  # Islamic GL text file
STORE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDNLGL/")
STORE_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------
# Helpers
# ----------------------------
def _fmt_DDMMYY8(d: date) -> str:
    # dd/mm/yy
    return d.strftime("%d/%m/%y")

def _parse_DDMMYY8(s: str) -> date:
    return datetime.strptime(s, "%d/%m/%y").date()

def _round_thousands_div_thousand(expr: pl.Expr) -> pl.Expr:
    # SAS: ROUND(x, 1000.) / 1000  ==> round to nearest 1000 then divide by 1000
    return (expr / 1000).round(0)

def _write_store(df: pl.DataFrame, name: str) -> None:
    out = STORE_DIR / f"{name}.parquet"
    df.write_parquet(out)
    print(f"✓ Saved: {out}")

# ----------------------------
# Read GL text file (fixed-width format)
# ----------------------------
def read_gl_text_file(filepath: Path):
    """Read GL text file with fixed-width format for Islamic version"""
    
    if not filepath.exists():
        raise FileNotFoundError(f"GL file not found: {filepath}")
    
    with open(filepath, 'r') as f:
        lines = [line.rstrip('\n') for line in f.readlines() if line.strip()]
    
    print(f"Total lines: {len(lines)}")
    
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
        
        # Extract DATE (positions 20-28)
        date_str = line[20:28].strip() if len(line) > 28 else ''
        
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
        
        # Get date from header
        if header_date:
            yy = header_date[0:2]
            mm = header_date[2:4]
            dd = header_date[4:6]
            # Create date object from header
            gl_date = date(int(f"20{yy}"), int(mm), int(dd))
        else:
            # Use yesterday's date as fallback
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
        
        # Only include if GLITEM looks valid
        if glitem and glitem != '20260708':
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

# ----------------------------
# 1) Get REPTDATE (yesterday for Islamic version)
# ----------------------------
REPTDATE = datetime.now() - timedelta(days=1)
REPTDATE = REPTDATE.date()
REPTYEAR = REPTDATE.strftime("%Y")
REPTMON = REPTDATE.strftime("%m")
REPTDAY = REPTDATE.strftime("%d")
RDATE = _fmt_DDMMYY8(REPTDATE)  # dd/mm/yy

print("="*60)
print("ISLAMIC GL PROCESSING STARTED (EIIDNLGL)")
print("="*60)
print(f"Processing date: {REPTDATE}")
print(f"Store directory: {STORE_DIR}")
print("="*60)

# ----------------------------
# 2) Read GLFILE and derive GL date from first record
# ----------------------------
gl_df_raw = read_gl_text_file(GLFILE_TXT)

if gl_df_raw.height == 0:
    raise RuntimeError("GLFILE is empty.")

# Get GL date from first record's DATEX
GLDATE = _parse_DDMMYY8(gl_df_raw.select("DATEX").row(0)[0])
GL = _fmt_DDMMYY8(GLDATE)

print(f"GL Date from file: {GL}")
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
    # Mirror DATA step:
    # DATE = INPUT(DATEX, DDMMYY8.)
    # IF SIGN='-' THEN BALANCE = BALANCE*(-1)
    # Initialize WEEK, MONTH, QTR, HALFYR, YEAR, LAST, TOTAL to 0
    return (
        gl.with_columns(
            DATE = pl.col("DATEX").map_elements(_parse_DDMMYY8, return_dtype=pl.Date),
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
    # PROC SUMMARY NWAY; CLASS ITEM; VAR WEEK MONTH QTR HALFYR YEAR LAST BALANCE; OUTPUT SUM=;
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
    """
    Mirrors:
      WEEK    = ROUND(WEEK,1000.)/1000;
      ...
      BALANCE = ROUND(BALANCE,1000.)/1000;
      IF SUBSTR(ITEM,1,1)='A' THEN DO;
         IF SUBSTR(ITEM,2,1)='1' THEN OUTPUT STORE.GLRMP{pass}
         IF SUBSTR(ITEM,2,1)='2' THEN OUTPUT STORE.GLUTRMP{pass}
      END;
      ELSE DO;
         IF SUBSTR(ITEM,2,1)='1' THEN OUTPUT STORE.GLFXP{pass}
         IF SUBSTR(ITEM,2,1)='2' THEN OUTPUT STORE.GLUTFXP{pass}
      END;
    """
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

    # Create selectors
    first1 = rounded["ITEM"].str.slice(0, 1)
    second1 = rounded["ITEM"].str.slice(1, 1)

    # Partitions
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

# List all created parquet files
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
