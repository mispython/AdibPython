import polars as pl
import duckdb
from pathlib import Path
import datetime
from datetime import date
import re

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
# 2. BRH PROCESSING (Based on your sample BRH file)
# ------------------------------------------------------------
def process_brh(brh_path):
    """Process BRH flat file, remove BRSTAT='C'"""
    lines = []
    with open(brh_path, 'r') as f:
        for line in f:
            # Based on your sample: B001 PCS   BANK-ATMC                             C
            # BRSTAT seems to be at position around 70-80
            brstat = line[70:71] if len(line) > 70 else ''
            if brstat != 'C':
                branch = line[0:4].strip()    # B001
                brcd = line[5:8].strip()      # PCS
                lines.append({'BRANCH': branch, 'BRCD': brcd})
    
    if not lines:
        return pl.DataFrame({'BRANCH': [], 'BRCD': []})
    
    return pl.DataFrame(lines)

# ------------------------------------------------------------
# 3. DATE EXTRACTION FROM TEXT FILES
# ------------------------------------------------------------
def extract_elds_date(file_path):
    """Extract date from ELDSTXT/ELDSTX2 first line"""
    try:
        with open(file_path, 'r') as f:
            line = f.readline()
            if len(line) >= 62:
                # Try to find date pattern
                date_match = re.search(r'(\d{2})/(\d{2})/(\d{4})', line)
                if date_match:
                    dd, mm, yy = date_match.groups()
                    return date(int(yy), int(mm), int(dd))
                
                # Alternative: fixed positions
                if len(line) >= 62:
                    try:
                        dd = int(line[52:54])  # Positions 53-54
                        mm = int(line[55:57])  # Positions 56-57
                        yy = int(line[58:62])  # Positions 59-62
                        return date(yy, mm, dd)
                    except:
                        pass
    except Exception as e:
        print(f"Warning: Could not extract date from {file_path}: {e}")
    return None

# ------------------------------------------------------------
# 4. PROCESS ELN1 (Fixed version)
# ------------------------------------------------------------
def process_eln1(file_path):
    """Process ELN1 from ELDSTXT starting at line 2"""
    # Read all lines
    lines = []
    with open(file_path, 'r', encoding='utf8') as f:
        all_lines = f.readlines()
        
    # Skip first line (used for date extraction)
    for line in all_lines[1:]:
        if len(line.strip()) == 0:
            continue
            
        # Parse fixed-width positions - USING STRING SLICING
        aano = line[0:13].strip() if len(line) >= 13 else ''
        brcd = line[79:82].strip() if len(line) >= 82 else ''
        
        # Parse numeric fields with comma removal
        amountx_str = line[132:147].strip() if len(line) >= 147 else ''
        amountx = float(amountx_str.replace(',', '')) if amountx_str else None
        
        gincome_str = line[635:650].strip() if len(line) >= 650 else ''
        gincome = float(gincome_str.replace(',', '')) if gincome_str else None
        
        spaamt_str = line[650:665].strip() if len(line) >= 665 else ''
        spaamt = float(spaamt_str.replace(',', '')) if spaamt_str else None
        
        # Parse other fields
        facili = line[99:129].strip() if len(line) >= 129 else ''
        status = line[991:1016].strip().upper() if len(line) >= 1016 else ''
        
        lines.append({
            'AANO': aano,
            'BRCD': brcd,
            'FACILI': facili,
            'AMOUNTX': amountx,
            'GINCOME': gincome,
            'SPAAMT': spaamt,
            'STATUS': status
        })
    
    if not lines:
        return pl.DataFrame(schema={
            'AANO': pl.Utf8, 'BRCD': pl.Utf8, 'FACILI': pl.Utf8,
            'AMOUNTX': pl.Float64, 'GINCOME': pl.Float64, 
            'SPAAMT': pl.Float64, 'STATUS': pl.Utf8
        })
    
    return pl.DataFrame(lines)

# ------------------------------------------------------------
# 5. PROCESS ELN2 (Simplified version)
# ------------------------------------------------------------
def process_eln2(file_path):
    """Process ELN2 from ELDSTX2 starting at line 2"""
    lines = []
    with open(file_path, 'r', encoding='utf8') as f:
        all_lines = f.readlines()
    
    for line in all_lines[1:]:
        if len(line.strip()) == 0:
            continue
            
        # Parse key columns
        aano = line[0:13].strip() if len(line) >= 13 else ''
        status = line[855:880].strip().upper() if len(line) >= 880 else ''
        
        # Parse numeric fields
        chnglmt_str = line[813:828].strip() if len(line) >= 828 else ''
        chnglmt = float(chnglmt_str.replace(',', '')) if chnglmt_str else None
        
        felimit_str = line[29:44].strip() if len(line) >= 44 else ''
        felimit = float(felimit_str.replace(',', '')) if felimit_str else None
        
        # Parse date fields
        sdd = line[120:122].strip() if len(line) >= 122 else ''
        smm = line[123:125].strip() if len(line) >= 125 else ''
        syy = line[126:130].strip() if len(line) >= 130 else ''
        
        idd = line[159:161].strip() if len(line) >= 161 else ''
        imm = line[162:164].strip() if len(line) >= 164 else ''
        iyy = line[165:169].strip() if len(line) >= 169 else ''
        
        # Convert dates
        sbdate = None
        if sdd and smm and syy:
            try:
                sbdate = date(int(syy), int(smm), int(sdd))
            except:
                pass
        
        iddate = None
        if idd and imm and iyy:
            try:
                iddate = date(int(iyy), int(imm), int(idd))
            except:
                pass
        
        lines.append({
            'AANO': aano,
            'STATUS': status,
            'CHNGLMT': chnglmt,
            'FELIMIT': felimit,
            'SBDATE': sbdate,
            'IDDATE': iddate,
            'AMOUNT': chnglmt  # Initialize AMOUNT with CHNGLMT
        })
    
    if not lines:
        return pl.DataFrame(schema={
            'AANO': pl.Utf8, 'STATUS': pl.Utf8, 'CHNGLMT': pl.Float64,
            'FELIMIT': pl.Float64, 'SBDATE': pl.Date, 'IDDATE': pl.Date,
            'AMOUNT': pl.Float64
        })
    
    return pl.DataFrame(lines)

# ------------------------------------------------------------
# 6. MAIN PROCESSING PIPELINE
# ------------------------------------------------------------
def process_loan_reports(base_path):
    """Main processing function"""
    
    # 1. Calculate reporting period
    macros = get_reptdate()
    NOWK = macros['NOWK']
    REPTMON = macros['REPTMON']
    REPTYEAR = macros['REPTYEAR']
    
    print(f"Processing for period: {REPTMON}/{REPTYEAR}, Week: {NOWK}")
    
    # 2. Process BRH
    print("Processing BRH...")
    brh_path = Path(base_path) / "BRH"
    brh_df = process_brh(brh_path)
    print(f"BRH records after filtering: {len(brh_df)}")
    
    # 3. Extract dates from ELDS files
    print("Extracting ELDS dates...")
    eldstxt_path = Path(base_path) / "ELDSTXT"
    eldstx2_path = Path(base_path) / "ELDSTX2"
    
    ELDSDT1 = extract_elds_date(eldstxt_path)
    ELDSDT2 = extract_elds_date(eldstx2_path)
    
    print(f"ELDSDT1: {ELDSDT1}, ELDSDT2: {ELDSDT2}")
    
    # 4. Process ELN1 and ELN2
    print("Processing ELN1...")
    eln1_df = process_eln1(eldstxt_path)
    print(f"ELN1 records: {len(eln1_df)}")
    
    print("Processing ELN2...")
    eln2_df = process_eln2(eldstx2_path)
    print(f"ELN2 records: {len(eln2_df)}")
    
    # 5. Merge ELN1 and ELN2
    print("Merging datasets...")
    
    # Sort by AANO, STATUS
    eln1_sorted = eln1_df.sort(['AANO', 'STATUS'])
    eln2_sorted = eln2_df.sort(['AANO', 'STATUS'])
    
    # Merge on AANO and STATUS
    sibc_df = eln1_sorted.join(eln2_sorted, on=['AANO', 'STATUS'], how='outer')
    print(f"After merge: {len(sibc_df)} records")
    
    # Apply SAS conditional logic
    print("Applying business logic...")
    sibc_df = sibc_df.with_columns([
        pl.when(
            (pl.col('STATUS') == 'APPLIED') & 
            ((pl.col('AMOUNT').is_null()) | (pl.col('AMOUNT') == 0))
        )
        .then(pl.col('AMOUNTX'))
        .otherwise(pl.col('AMOUNT'))
        .alias('AMOUNT')
    ])
    
    # Drop columns as in SAS
    sibc_df = sibc_df.drop(['AMOUNTX', 'CHNGLMT'])
    
    # Remove duplicates
    sibc_df = sibc_df.unique(subset=['AANO', 'STATUS'])
    print(f"After deduplication: {len(sibc_df)} records")
    
    # 6. Merge with BRH data
    print("Merging with branch data...")
    if len(brh_df) > 0:
        sibc_df = sibc_df.join(brh_df, on='BRCD', how='inner')
        print(f"After BRH merge: {len(sibc_df)} records")
    
    # Drop BRCD column as in SAS
    sibc_df = sibc_df.drop('BRCD')
    
    # 7. Save output
    output_name = f"SIBC{REPTMON}{REPTYEAR}{NOWK}"
    output_dir = Path(base_path) / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save to parquet (main output)
    parquet_path = output_dir / f"{output_name}.parquet"
    sibc_df.write_parquet(parquet_path)
    print(f"Saved to: {parquet_path}")
    
    # Save to CSV
    csv_path = output_dir / f"{output_name}.csv"
    sibc_df.write_csv(csv_path)
    print(f"Also saved to: {csv_path}")
    
    return sibc_df

# ------------------------------------------------------------
# 7. EXECUTE
# ------------------------------------------------------------
if __name__ == "__main__":
    # Set your base path - UPDATE THIS
    BASE_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/Job"
    
    try:
        print("Starting loan report processing...")
        result = process_loan_reports(BASE_PATH)
        
        print("\nProcessing complete!")
        print(f"Total records in final output: {len(result)}")
        print(f"\nFirst 5 records:")
        print(result.head(5))
        
    except FileNotFoundError as e:
        print(f"File not found error: {e}")
        print("Please check file paths in the code.")
    except Exception as e:
        print(f"Error during processing: {e}")
        import traceback
        traceback.print_exc()
