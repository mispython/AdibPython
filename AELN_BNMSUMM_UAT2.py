import polars as pl
import duckdb
from pathlib import Path
import datetime
from datetime import date
import tempfile

# ------------------------------------------------------------
# 1. REPTDATE CALCULATION (Same as SAS)
# ------------------------------------------------------------
def get_reptdate():
    today = date.today()
    day = today.day
    
    if 8 <= day <= 14:
        reptdate = date(today.year, today.month, 8)
        wk = '1'
    elif 15 <= day <= 21:
        reptdate = date(today.year, today.month, 15)
        wk = '2'
    elif 22 <= day <= 27:
        reptdate = date(today.year, today.month, 22)
        wk = '3'
    else:
        # Last day of previous month
        if today.month == 1:
            reptdate = date(today.year - 1, 12, 31)
        else:
            import calendar
            last_day = calendar.monthrange(today.year, today.month - 1)[1]
            reptdate = date(today.year, today.month - 1, last_day)
        wk = '4'
    
    return {
        'NOWK': wk,
        'RDATE': reptdate.strftime('%d%m%y'),
        'REPTMON': f"{reptdate.month:02d}",
        'REPTYEAR': str(reptdate.year)[-2:],
        'reptdate': reptdate
    }

# ------------------------------------------------------------
# 2. BRH PROCESSING (Flat file)
# ------------------------------------------------------------
def process_brh(brh_path):
    """Process BRH flat file, remove BRSTAT='C'"""
    # Read as text and parse fixed-width positions
    lines = []
    with open(brh_path, 'r') as f:
        for line in f:
            brstat = line[49] if len(line) > 49 else ''  # Position 50 (0-indexed)
            if brstat != 'C':
                branch = line[1:4].strip()  # Positions 2-4
                brcd = line[5:8].strip()    # Positions 6-8
                lines.append({'BRANCH': branch, 'BRCD': brcd})
    
    return pl.DataFrame(lines)

# ------------------------------------------------------------
# 3. DATE EXTRACTION FROM TEXT FILES (ELDSDT1/ELDSDT2)
# ------------------------------------------------------------
def extract_elds_date(file_path):
    """Extract date from ELDSTXT/ELDSTX2 first line position 53"""
    try:
        with open(file_path, 'r') as f:
            line = f.readline()
            if len(line) >= 62:  # Need up to position 62
                dd = int(line[52:54])  # Positions 53-54
                mm = int(line[55:57])  # Positions 56-57
                yy = int(line[58:62])  # Positions 59-62
                return date(yy, mm, dd)
    except:
        return None
    return None

# ------------------------------------------------------------
# 4. PROCESS ELN1 (from ELDSTXT text file)
# ------------------------------------------------------------
def process_eln1(file_path):
    """Process ELN1 from ELDSTXT starting at line 2"""
    # Skip first line (used for date extraction)
    df = pl.read_csv(
        file_path,
        skip_rows=1,
        has_header=False,
        new_columns=['raw_line'],
        encoding='utf8'
    )
    
    # Parse fixed-width columns
    df = df.with_columns([
        pl.col('raw_line').str.slice(0, 13).str.strip().alias('AANO'),
        pl.col('raw_line').str.slice(79, 3).str.strip().alias('BRCD'),
        pl.col('raw_line').str.slice(86, 10).str.strip().alias('FACCODE'),
        pl.col('raw_line').str.slice(99, 30).str.strip().alias('FACILI'),
        pl.col('raw_line').str.slice(132, 15).str.replace(',', '').str.strip().cast(pl.Float64).alias('AMOUNTX'),
        pl.col('raw_line').str.slice(150, 8).str.strip().alias('BNMEFF'),
        pl.col('raw_line').str.slice(161, 200).str.strip().str.to_uppercase().alias('APPRIC'),
        pl.col('raw_line').str.slice(364, 15).str.replace(',', '').str.strip().cast(pl.Float64).alias('AMTAPPLY'),
        pl.col('raw_line').str.slice(382, 200).str.strip().str.to_uppercase().alias('AVPRIC'),
        pl.col('raw_line').str.slice(585, 8).str.strip().alias('PRICING'),
        pl.col('raw_line').str.slice(596, 12).str.strip().alias('NEWIC'),
        pl.col('raw_line').str.slice(611, 3).str.strip().alias('CPARTY'),
        pl.col('raw_line').str.slice(617, 15).str.strip().str.to_uppercase().alias('LNTYPE'),
        pl.col('raw_line').str.slice(635, 15).str.replace(',', '').str.strip().cast(pl.Float64).alias('GINCOME'),
        pl.col('raw_line').str.slice(650, 15).str.replace(',', '').str.strip().cast(pl.Float64).alias('SPAAMT'),
        pl.col('raw_line').str.slice(665, 100).str.strip().str.to_uppercase().alias('CPRELAT'),
        pl.col('raw_line').str.slice(765, 100).str.strip().str.to_uppercase().alias('CPRELAS'),
        pl.col('raw_line').str.slice(889, 50).str.strip().str.to_uppercase().alias('CPSTAFF'),
        pl.col('raw_line').str.slice(939, 3).str.strip().str.to_uppercase().alias('CPDITOR'),
        pl.col('raw_line').str.slice(942, 5).str.strip().str.to_uppercase().alias('CPSTFID'),
        pl.col('raw_line').str.slice(947, 11).str.strip().str.to_uppercase().alias('CPBRHO'),
        pl.col('raw_line').str.slice(991, 25).str.strip().str.to_uppercase().alias('STATUS')
    ]).drop('raw_line')
    
    return df

# ------------------------------------------------------------
# 5. PROCESS ELN2 (from ELDSTX2 text file)
# ------------------------------------------------------------
def process_eln2(file_path):
    """Process ELN2 from ELDSTX2 starting at line 2"""
    # Skip first line
    df = pl.read_csv(
        file_path,
        skip_rows=1,
        has_header=False,
        new_columns=['raw_line'],
        encoding='utf8'
    )
    
    # Parse key columns (simplified - focusing on essential fields)
    df = df.with_columns([
        pl.col('raw_line').str.slice(0, 13).str.strip().alias('AANO'),
        pl.col('raw_line').str.slice(29, 15).str.replace(',', '').str.strip().cast(pl.Float64).alias('FELIMIT'),
        pl.col('raw_line').str.slice(47, 15).str.replace(',', '').str.strip().cast(pl.Float64).alias('TRLIMIT'),
        pl.col('raw_line').str.slice(65, 4).str.strip().cast(pl.Int64).alias('CUSTCODE'),
        pl.col('raw_line').str.slice(106, 11).str.replace(',', '').str.strip().cast(pl.Float64).alias('TURNOVER'),
        pl.col('raw_line').str.slice(795, 15).str.replace(',', '').str.strip().cast(pl.Float64).alias('EXSTLMT'),
        pl.col('raw_line').str.slice(813, 15).str.replace(',', '').str.strip().cast(pl.Float64).alias('CHNGLMT'),
        pl.col('raw_line').str.slice(855, 25).str.strip().str.to_uppercase().alias('STATUS'),
        pl.col('raw_line').str.slice(925, 150).str.strip().str.to_uppercase().alias('NAME')
    ])
    
    # Parse date components and create date columns
    # Using DuckDB for easier date construction
    conn = duckdb.connect()
    conn.register('df_temp', df)
    
    # Parse multiple dates (simplified to key dates only)
    result = conn.execute("""
        SELECT 
            AANO, FELIMIT, TRLIMIT, CUSTCODE, TURNOVER, 
            EXSTLMT, CHNGLMT, STATUS, NAME,
            -- Parse SBDATE (Submission Date)
            CASE 
                WHEN SUBSTR(raw_line, 120, 2) != '' AND SUBSTR(raw_line, 123, 2) != '' AND SUBSTR(raw_line, 126, 4) != ''
                THEN make_date(
                    CAST(SUBSTR(raw_line, 126, 4) AS INTEGER),
                    CAST(SUBSTR(raw_line, 123, 2) AS INTEGER),
                    CAST(SUBSTR(raw_line, 120, 2) AS INTEGER)
                )
                ELSE NULL 
            END as SBDATE,
            
            -- Parse IDDATE (Decision Date)
            CASE 
                WHEN SUBSTR(raw_line, 159, 2) != '' AND SUBSTR(raw_line, 162, 2) != '' AND SUBSTR(raw_line, 165, 4) != ''
                THEN make_date(
                    CAST(SUBSTR(raw_line, 165, 4) AS INTEGER),
                    CAST(SUBSTR(raw_line, 162, 2) AS INTEGER),
                    CAST(SUBSTR(raw_line, 159, 2) AS INTEGER)
                )
                ELSE NULL 
            END as IDDATE,
            
            -- Parse APVDTE1 (Approval Date 1)
            CASE 
                WHEN SUBSTR(raw_line, 198, 2) != '' AND SUBSTR(raw_line, 201, 2) != '' AND SUBSTR(raw_line, 204, 4) != ''
                THEN make_date(
                    CAST(SUBSTR(raw_line, 204, 4) AS INTEGER),
                    CAST(SUBSTR(raw_line, 201, 2) AS INTEGER),
                    CAST(SUBSTR(raw_line, 198, 2) AS INTEGER)
                )
                ELSE NULL 
            END as APVDTE1
            
        FROM df_temp
    """).pl()
    
    conn.close()
    
    # Add AMOUNT column from CHNGLMT
    result = result.with_columns(pl.col('CHNGLMT').alias('AMOUNT'))
    
    return result

# ------------------------------------------------------------
# 6. READ SAS7BDAT FILE (ELDS2)
# ------------------------------------------------------------
def read_sas7bdat(sas_path):
    """Read SAS7BDAT file if needed (ELDS2 reference)"""
    try:
        # Using pandas with sas7bdat or pyreadstat
        import pandas as pd
        import pyreadstat
        df_sas, meta = pyreadstat.read_sas7bdat(sas_path)
        return pl.from_pandas(df_sas)
    except ImportError:
        print("Install pyreadstat: pip install pyreadstat")
        return None

# ------------------------------------------------------------
# 7. MAIN PROCESSING PIPELINE
# ------------------------------------------------------------
def process_loan_reports(base_path):
    """Main processing function - follows SAS logic"""
    
    # 1. Calculate reporting period
    macros = get_reptdate()
    NOWK = macros['NOWK']
    REPTMON = macros['REPTMON']
    REPTYEAR = macros['REPTYEAR']
    
    # 2. Process BRH (remove BRSTAT='C')
    print("Processing BRH...")
    brh_path = Path(base_path) / "BRH.flat"
    brh_df = process_brh(brh_path)
    
    # 3. Extract dates from ELDS files
    print("Extracting ELDS dates...")
    eldstxt_path = Path(base_path) / "ELDSTXT.txt"
    eldstx2_path = Path(base_path) / "ELDSTX2.txt"
    
    ELDSDT1 = extract_elds_date(eldstxt_path)
    ELDSDT2 = extract_elds_date(eldstx2_path)
    
    print(f"ELDSDT1: {ELDSDT1}, ELDSDT2: {ELDSDT2}")
    
    # 4. Process ELN1 and ELN2
    print("Processing ELN1...")
    eln1_df = process_eln1(eldstxt_path)
    
    print("Processing ELN2...")
    eln2_df = process_eln2(eldstx2_path)
    
    # 5. Merge ELN1 and ELN2 (equivalent to SAS DATA SIBC)
    print("Merging datasets...")
    
    # Sort by AANO, STATUS
    eln1_sorted = eln1_df.sort(['AANO', 'STATUS'])
    eln2_sorted = eln2_df.sort(['AANO', 'STATUS'])
    
    # Merge on AANO and STATUS
    sibc_df = eln1_sorted.join(eln2_sorted, on=['AANO', 'STATUS'], how='outer')
    
    # Apply SAS conditional logic: IF STATUS='APPLIED' AND AMOUNT missing/0 THEN AMOUNT=AMOUNTX
    sibc_df = sibc_df.with_columns(
        pl.when(
            (pl.col('STATUS') == 'APPLIED') & 
            (pl.col('AMOUNT').is_null() | (pl.col('AMOUNT') == 0))
        )
        .then(pl.col('AMOUNTX'))
        .otherwise(pl.col('AMOUNT'))
        .alias('AMOUNT')
    ).drop(['AMOUNTX', 'CHNGLMT'])
    
    # Remove duplicates (equivalent to NODUPKEY)
    sibc_df = sibc_df.unique(subset=['AANO', 'STATUS'])
    
    # 6. Merge with BRH data
    print("Merging with branch data...")
    sibc_df = sibc_df.sort('BRCD')
    brh_df = brh_df.sort('BRCD')
    
    sibc_final = sibc_df.join(brh_df, on='BRCD', how='inner').drop('BRCD')
    
    # 7. Save output in Parquet format
    output_name = f"SIBC{REPTMON}{REPTYEAR}{NOWK}"
    output_path = Path(base_path) / "output" / f"{output_name}.parquet"
    
    # Create output directory
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"Saving to: {output_path}")
    sibc_final.write_parquet(output_path)
    
    # 8. Optional: Also save to CSV
    csv_path = Path(base_path) / "output" / f"{output_name}.csv"
    sibc_final.write_csv(csv_path)
    
    print(f"Processing complete. Output: {output_name}")
    print(f"Records processed: {len(sibc_final)}")
    
    return sibc_final

# ------------------------------------------------------------
# 8. EXECUTE
# ------------------------------------------------------------
if __name__ == "__main__":
    # Set your base path
    BASE_PATH = "/path/to/your/data"
    
    try:
        result = process_loan_reports(BASE_PATH)
        
        # Show summary
        print("\nResult Summary:")
        print(f"Columns: {result.columns[:10]}...")  # Show first 10 columns
        print(f"First 3 rows:")
        print(result.head(3))
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
