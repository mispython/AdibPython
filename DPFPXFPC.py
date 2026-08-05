import polars as pl
from datetime import datetime, timedelta
import os
import sys
from pathlib import Path

# Configuration
BASE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIVMNLGL")
GLFILE_PATH = BASE / "glfile.txt"
STORE_OUT = BASE
OUTPUT = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIVMNLGL/"

# Calculate reptdate as yesterday
reptdate = datetime.now() - timedelta(days=1)
reptyear = f"{reptdate.year:04d}"
reptmon = f"{reptdate.month:02d}"
reptday = f"{reptdate.day:02d}"
rdate = f"{reptdate.day:02d}{reptdate.month:02d}{reptdate.year%100:02d}"

print(f"Current date: {datetime.now().strftime('%Y-%m-%d')}")
print(f"Report date (yesterday): {reptdate.strftime('%Y-%m-%d')}")
print(f"Expected date format (DDMMYY): {rdate}")

# Helper functions
def parse_date_flexible(s: str) -> datetime:
    """Parse date in various formats"""
    s = str(s).strip()
    
    # Try DD/MM/YY format first
    try:
        if '/' in s:
            parts = s.split('/')
            if len(parts) == 3:
                dd, mm, yy2 = int(parts[0]), int(parts[1]), int(parts[2])
                yy = 1900 + yy2 if yy2 >= 50 else 2000 + yy2
                return datetime(yy, mm, dd)
    except:
        pass
    
    # Try DDMMYY8 format
    try:
        if len(s) >= 6 and '/' not in s:
            dd, mm, yy2 = int(s[0:2]), int(s[2:4]), int(s[4:6])
            yy = 1900 + yy2 if yy2 >= 50 else 2000 + yy2
            return datetime(yy, mm, dd)
    except:
        pass
    
    # Return a default date if parsing fails
    print(f"Warning: Could not parse date '{s}'")
    return datetime(2000, 1, 1)

def round_thousands(df: pl.DataFrame, cols: list) -> pl.DataFrame:
    """Round values to thousands with 3 decimal places"""
    return df.with_columns([
        ((pl.col(c).round(0) / 1000).round(3)).alias(c) for c in cols
    ])

def save_as_sas(df_polars, sas_path, dataset_name):
    """Save Polars DataFrame as SAS dataset using saspy"""
    try:
        import saspy
        
        # Convert Polars to Pandas
        df_pandas = df_polars.to_pandas()
        
        # Initialize SAS session
        sas = saspy.SASsession()
        
        # Upload dataframe to SAS
        sas_df = sas.df2sd(df_pandas, dataset_name)
        
        # Save as permanent SAS dataset
        sas.saslib('mylib', path=str(sas_path.parent))
        sas.submit(f"""
            data mylib.{dataset_name};
                set {dataset_name};
            run;
        """)
        
        print(f"Saved SAS dataset: {sas_path}")
        sas.endsas()
        
    except ImportError:
        print("saspy not installed. Saving as CSV instead...")
        csv_path = str(sas_path).replace('.sas7bdat', '.csv')
        df_polars.write_csv(csv_path)
        print(f"Saved CSV file: {csv_path}")
    except Exception as e:
        print(f"Error saving SAS file: {e}")
        try:
            csv_path = str(sas_path).replace('.sas7bdat', '.csv')
            df_polars.write_csv(csv_path)
            print(f"Saved CSV file as fallback: {csv_path}")
        except:
            print("Could not save file in any format")

def save_outputs(df, name, date_str):
    """Save DataFrame in both SAS7BDAT and Parquet formats"""
    if len(df) == 0:
        print(f"No data for {name}{date_str}")
        return
        
    # Save as Parquet
    parquet_path = STORE_OUT / f"{name}{date_str}.parquet"
    df.write_parquet(str(parquet_path))
    print(f"Saved Parquet: {parquet_path}")
    
    # Save as SAS7BDAT
    sas_path = STORE_OUT / f"{name}{date_str}.sas7bdat"
    save_as_sas(df, sas_path, f"{name}{date_str}")

def split_and_write(R: pl.DataFrame, stub: str, y: str, m: str, d: str):
    """Split results by category and save"""
    date_str = f"{y}{m}{d}"
    
    if len(R) == 0:
        print(f"No results for stub {stub}")
        return
    
    T = R.with_columns([
        pl.col("ITEM").str.slice(0, 1).alias("S1"),
        pl.col("ITEM").str.slice(1, 1).alias("S2"),
    ])
    
    GLRMP = T.filter((pl.col("S1") == "A") & (pl.col("S2") == "1")).drop(["S1", "S2"])
    GLUTRMP = T.filter((pl.col("S1") == "A") & (pl.col("S2") == "2")).drop(["S1", "S2"])
    GLFXP = T.filter((pl.col("S1") != "A") & (pl.col("S2") == "1")).drop(["S1", "S2"])
    GLUTFXP = T.filter((pl.col("S1") != "A") & (pl.col("S2") == "2")).drop(["S1", "S2"])
    
    # Save each category
    if len(GLRMP) > 0:
        save_outputs(GLRMP, f"GLRMP{stub}", date_str)
        print(f"\nGLRMP{stub}{date_str}:")
        print(GLRMP)
    
    if len(GLFXP) > 0:
        save_outputs(GLFXP, f"GLFXP{stub}", date_str)
        print(f"\nGLFXP{stub}{date_str}:")
        print(GLFXP)
    
    if len(GLUTRMP) > 0:
        save_outputs(GLUTRMP, f"GLUTRMP{stub}", date_str)
        print(f"\nGLUTRMP{stub}{date_str}:")
        print(GLUTRMP)
    
    if len(GLUTFXP) > 0:
        save_outputs(GLUTFXP, f"GLUTFXP{stub}", date_str)
        print(f"\nGLUTFXP{stub}{date_str}:")
        print(GLUTFXP)

# Read GL file
print("\nReading GL file...")
with open(GLFILE_PATH, 'r') as f:
    lines = f.readlines()

# First line contains the date (YYYYMMDD format)
if len(lines) > 0:
    date_str = lines[0].strip()
    
    # Parse date from first line (YYYYMMDD)
    if len(date_str) >= 8:
        try:
            yy = int(date_str[0:4])
            mm = int(date_str[4:6])
            dd = int(date_str[6:8])
            gl_date = datetime(yy, mm, dd)
            gl = gl_date.strftime('%d%m%y')
            
            print(f"File date: {gl_date.strftime('%Y-%m-%d')} (DDMMYY: {gl})")
            print(f"Expected date (DDMMYY): {rdate}")
            
            # Check if the file date matches expected date
            if gl != rdate:
                print(f"WARNING: GL file extraction date ({gl}) does not match expected date ({rdate})")
                print(f"Using file date for processing...")
                reptdate = gl_date
                reptyear = f"{reptdate.year:04d}"
                reptmon = f"{reptdate.month:02d}"
                reptday = f"{reptdate.day:02d}"
                rdate = f"{reptdate.day:02d}{reptdate.month:02d}{reptdate.year%100:02d}"
                
        except ValueError as e:
            print(f"Error parsing date: {e}")
            sys.exit(1)
    else:
        print("First line doesn't contain valid date")
        sys.exit(1)

# Process data lines - Investment version with fixed-width parsing
# Based on the original SAS code:
# INPUT @002 GLITEM $15. @021 DATEX $8. @045 BALANCE COMMA17.2 @062 SIGN $1.
data_lines = lines[1:]
print(f"\nProcessing {len(data_lines)} data lines")

# Parse using fixed positions (as per SAS code)
records = []
for line_num, line in enumerate(data_lines, 1):
    line = line.rstrip('\n')
    if not line.strip():
        continue
    
    # Skip separator lines
    if line.strip() == '-':
        continue
    
    try:
        # Extract fields based on SAS input positions
        # @002 GLITEM $15. - starts at position 2 (0-indexed: 1), length 15
        glitem = line[1:16].strip() if len(line) >= 16 else line[1:].strip()
        
        # @021 DATEX $8. - starts at position 21 (0-indexed: 20), length 8
        datex = line[20:28].strip() if len(line) >= 28 else ""
        
        # @045 BALANCE COMMA17.2 - starts at position 45 (0-indexed: 44), length 17
        balance_str = line[44:61].strip() if len(line) >= 61 else ""
        
        # @062 SIGN $1. - starts at position 62 (0-indexed: 61), length 1
        sign = line[61:62].strip() if len(line) >= 62 else "+"
        
        if not glitem or not balance_str:
            continue
        
        # Clean balance string
        balance_str = balance_str.replace(',', '')
        
        # Handle trailing minus sign
        if balance_str.endswith('-'):
            sign = '-'
            balance_str = balance_str[:-1]
        
        balance = float(balance_str)
        
        records.append({
            'GLITEM': glitem,
            'DATEX': datex,
            'SIGN': sign,
            'BALANCE': balance
        })
        
    except (ValueError, IndexError) as e:
        print(f"Line {line_num}: Parse error - {e}: {line[:80]}")
        continue

# Create DataFrame
if records:
    df_gl = pl.DataFrame(records)
    print(f"\nCreated DataFrame with {len(df_gl)} records")
    print(f"Sample GLITEMs: {df_gl['GLITEM'].unique().to_list()}")
    print(f"\nFirst few rows:")
    print(df_gl.head(10))
else:
    print("No records found in the file")
    sys.exit(1)

# Create DETAIL with proper date, sign adjustment, and ITEM mappings
DETAIL = (
    df_gl
    .with_columns([
        pl.col('DATEX').map_elements(lambda x: parse_date_flexible(str(x)), return_dtype=pl.Datetime).alias('DATE'),
        pl.when(pl.col('SIGN') == '-')
          .then(-pl.col('BALANCE'))
          .otherwise(pl.col('BALANCE'))
          .alias('BALANCE'),
    ])
    # Flip positive BALANCE to negative for certain GLITEMs (Investment specific)
    .with_columns(
        pl.when(pl.col("GLITEM").is_in(["S-RCF", "S-GUARANTEE", "S-SM F", "S-TLF", "S-BA F"]) & (pl.col("BALANCE") > 0))
          .then(-pl.col("BALANCE"))
          .otherwise(pl.col("BALANCE"))
          .alias("BALANCE")
    )
    # ITEM + WEEK / MONTH assignments
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
    ])
    # BALANCE = SUM(WEEK,MONTH,QTR,HALFYR,YEAR,LAST,TOTAL)
    .with_columns(
        pl.sum_horizontal([
            pl.col("WEEK").fill_null(0.0),
            pl.col("MONTH").fill_null(0.0),
            pl.lit(0.0).alias("QTR"),
            pl.lit(0.0).alias("HALFYR"),
            pl.lit(0.0).alias("YEAR"),
            pl.lit(0.0).alias("LAST"),
            pl.lit(0.0).alias("TOTAL"),
        ]).alias("BALANCE")
    )
    .filter(pl.col("ITEM") != " ")
)

# PROC SUMMARY NWAY (sum by ITEM)
SUMV = ["WEEK", "MONTH", "QTR", "HALFYR", "YEAR", "LAST", "BALANCE"]
DF = DETAIL
for v in SUMV:
    if v not in DF.columns:
        DF = DF.with_columns(pl.lit(0.0).alias(v))
    else:
        DF = DF.with_columns(pl.col(v).fill_null(0.0))

# Group by ITEM and sum
GL_SUM = DF.group_by("ITEM").agg([pl.col(v).sum().alias(v) for v in SUMV])

# Round to thousands
R = round_thousands(GL_SUM, SUMV)

# Split and write outputs (only one pass for Investment version)
print("\nProcessing Investment outputs...")
split_and_write(R, stub="1", y=reptyear, m=reptmon, d=reptday)

print("\nProcessing complete!")
