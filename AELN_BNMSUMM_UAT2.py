import polars as pl
import duckdb
from pathlib import Path
import datetime
from datetime import date
import re
import sys

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
# 2. BRH PROCESSING (From /sasdata/rawdata/lookup/LKP_BRANCH)
# ------------------------------------------------------------
def process_brh(brh_path):
    """Process BRH flat file, remove BRSTAT='C'"""
    print(f"Reading BRH from: {brh_path}")
    lines = []
    try:
        with open(brh_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.rstrip('\n')
                if not line.strip():
                    continue
                    
                # Try to find BRSTAT position - looking for 'C' or 'O' at end
                # Based on your sample: B001 PCS   BANK-ATMC                             C
                brstat = line[-1] if len(line) > 0 else ''
                
                if brstat != 'C':
                    # Parse first few columns
                    parts = line.split()
                    if len(parts) >= 2:
                        branch = parts[0]  # B001
                        brcd = parts[1]    # PCS
                        lines.append({'BRANCH': branch, 'BRCD': brcd})
                    else:
                        print(f"Warning: Line {line_num} has unexpected format: {line[:50]}...")
    
    except FileNotFoundError:
        print(f"ERROR: BRH file not found at {brh_path}")
        return pl.DataFrame({'BRANCH': [], 'BRCD': []})
    except Exception as e:
        print(f"ERROR reading BRH: {e}")
        return pl.DataFrame({'BRANCH': [], 'BRCD': []})
    
    print(f"BRH records after filtering BRSTAT='C': {len(lines)}")
    
    if not lines:
        return pl.DataFrame({'BRANCH': [], 'BRCD': []})
    
    return pl.DataFrame(lines)

# ------------------------------------------------------------
# 3. DATE EXTRACTION FROM TEXT FILES
# ------------------------------------------------------------
def extract_elds_date(file_path, file_name):
    """Extract date from ELDSTXT/ELDSTX2 first line"""
    try:
        if not Path(file_path).exists():
            print(f"Warning: File not found: {file_path}")
            return None
            
        with open(file_path, 'r', encoding='latin-1') as f:
            line = f.readline()
            print(f"First line of {file_name}: {line[:100]}...")
            
            if len(line) >= 62:
                # Try to find date in DD/MM/YYYY or DD-MM-YYYY format
                date_patterns = [
                    r'(\d{2})/(\d{2})/(\d{4})',  # DD/MM/YYYY
                    r'(\d{2})-(\d{2})-(\d{4})',  # DD-MM-YYYY
                    r'(\d{2})\.(\d{2})\.(\d{4})' # DD.MM.YYYY
                ]
                
                for pattern in date_patterns:
                    date_match = re.search(pattern, line)
                    if date_match:
                        dd, mm, yy = date_match.groups()
                        return date(int(yy), int(mm), int(dd))
                
                # Alternative: fixed positions (53-62 as in SAS)
                try:
                    if len(line) >= 62:
                        # SAS uses position 53-54 for DD, 56-57 for MM, 59-62 for YYYY
                        dd = int(line[52:54])  # Positions 53-54 (0-indexed)
                        mm = int(line[55:57])  # Positions 56-57
                        yy = int(line[58:62])  # Positions 59-62
                        return date(yy, mm, dd)
                except ValueError as e:
                    print(f"Warning: Could not parse fixed position date in {file_name}: {e}")
                    print(f"Line segment: {line[52:62]}")
                    
    except Exception as e:
        print(f"Warning: Could not extract date from {file_path}: {e}")
    
    return None

# ------------------------------------------------------------
# 4. PROCESS ELN1 (From ELDSTXT)
# ------------------------------------------------------------
def process_eln1(file_path):
    """Process ELN1 from ELDSTXT starting at line 2"""
    print(f"Processing ELN1 from: {file_path}")
    lines = []
    
    try:
        with open(file_path, 'r', encoding='latin-1') as f:
            all_lines = f.readlines()
        
        if len(all_lines) < 2:
            print(f"Warning: {file_path} has less than 2 lines")
            return pl.DataFrame()
            
        print(f"Total lines in ELDSTXT: {len(all_lines)}")
        
        # Skip first line (used for date extraction)
        for line_num, line in enumerate(all_lines[1:], 2):
            line = line.rstrip('\n')
            if len(line.strip()) == 0:
                continue
                
            # Parse fixed-width positions
            aano = line[0:13].strip() if len(line) >= 13 else ''
            brcd = line[79:82].strip() if len(line) >= 82 else ''
            
            # Parse numeric fields with comma removal
            amountx_str = line[132:147].strip() if len(line) >= 147 else ''
            amountx = None
            if amountx_str:
                try:
                    amountx = float(amountx_str.replace(',', '').replace(' ', ''))
                except:
                    amountx = None
            
            gincome_str = line[635:650].strip() if len(line) >= 650 else ''
            gincome = None
            if gincome_str:
                try:
                    gincome = float(gincome_str.replace(',', '').replace(' ', ''))
                except:
                    gincome = None
            
            spaamt_str = line[650:665].strip() if len(line) >= 665 else ''
            spaamt = None
            if spaamt_str:
                try:
                    spaamt = float(spaamt_str.replace(',', '').replace(' ', ''))
                except:
                    spaamt = None
            
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
            
            # Show progress for large files
            if line_num % 1000 == 0:
                print(f"  Processed {line_num} lines...")
    
    except FileNotFoundError:
        print(f"ERROR: ELDSTXT file not found at {file_path}")
        return pl.DataFrame()
    except Exception as e:
        print(f"ERROR processing ELN1: {e}")
        import traceback
        traceback.print_exc()
        return pl.DataFrame()
    
    print(f"ELN1 records processed: {len(lines)}")
    
    if not lines:
        return pl.DataFrame()
    
    return pl.DataFrame(lines)

# ------------------------------------------------------------
# 5. PROCESS ELN2 (From ELDSTX2)
# ------------------------------------------------------------
def process_eln2(file_path):
    """Process ELN2 from ELDSTX2 starting at line 2"""
    print(f"Processing ELN2 from: {file_path}")
    lines = []
    
    try:
        with open(file_path, 'r', encoding='latin-1') as f:
            all_lines = f.readlines()
        
        if len(all_lines) < 2:
            print(f"Warning: {file_path} has less than 2 lines")
            return pl.DataFrame()
            
        print(f"Total lines in ELDSTX2: {len(all_lines)}")
        
        for line_num, line in enumerate(all_lines[1:], 2):
            line = line.rstrip('\n')
            if len(line.strip()) == 0:
                continue
                
            # Parse key columns
            aano = line[0:13].strip() if len(line) >= 13 else ''
            status = line[855:880].strip().upper() if len(line) >= 880 else ''
            
            # Parse numeric fields
            chnglmt_str = line[813:828].strip() if len(line) >= 828 else ''
            chnglmt = None
            if chnglmt_str:
                try:
                    chnglmt = float(chnglmt_str.replace(',', '').replace(' ', ''))
                except:
                    chnglmt = None
            
            felimit_str = line[29:44].strip() if len(line) >= 44 else ''
            felimit = None
            if felimit_str:
                try:
                    felimit = float(felimit_str.replace(',', '').replace(' ', ''))
                except:
                    felimit = None
            
            # Parse date fields (SBDATE - Submission Date)
            sdd = line[120:122].strip() if len(line) >= 122 else ''
            smm = line[123:125].strip() if len(line) >= 125 else ''
            syy = line[126:130].strip() if len(line) >= 130 else ''
            
            # Parse date fields (IDDATE - Decision Date)
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
            
            # Show progress for large files
            if line_num % 1000 == 0:
                print(f"  Processed {line_num} lines...")
    
    except FileNotFoundError:
        print(f"ERROR: ELDSTX2 file not found at {file_path}")
        return pl.DataFrame()
    except Exception as e:
        print(f"ERROR processing ELN2: {e}")
        import traceback
        traceback.print_exc()
        return pl.DataFrame()
    
    print(f"ELN2 records processed: {len(lines)}")
    
    if not lines:
        return pl.DataFrame()
    
    return pl.DataFrame(lines)

# ------------------------------------------------------------
# 6. MAIN PROCESSING PIPELINE
# ------------------------------------------------------------
def process_loan_reports():
    """Main processing function"""
    
    # 1. Calculate reporting period
    print("=" * 60)
    print("Starting EIBWSIBC Report Processing")
    print("=" * 60)
    
    macros = get_reptdate()
    NOWK = macros['NOWK']
    REPTMON = macros['REPTMON']
    REPTYEAR = macros['REPTYEAR']
    
    print(f"Processing for period: {REPTMON}/{REPTYEAR}, Week: {NOWK}")
    
    # 2. Define file paths
    # ELDS files from /stgsrcsys/host/uat
    eldstxt_path = Path("/stgsrcsys/host/uat/ELDSTXT")
    eldstx2_path = Path("/stgsrcsys/host/uat/ELDSTX2")
    
    # BRH file from /sasdata/rawdata/lookup/LKP_BRANCH
    brh_path = Path("/sasdata/rawdata/lookup/LKP_BRANCH")
    
    # 3. Check if input files exist
    missing_files = []
    if not eldstxt_path.exists():
        missing_files.append(str(eldstxt_path))
    if not eldstx2_path.exists():
        missing_files.append(str(eldstx2_path))
    if not brh_path.exists():
        missing_files.append(str(brh_path))
    
    if missing_files:
        print("ERROR: Missing input files:")
        for f in missing_files:
            print(f"  - {f}")
        print("Please check file paths and permissions.")
        return None
    
    # 4. Extract dates from ELDS files
    print("\n" + "-" * 60)
    print("Extracting ELDS dates...")
    ELDSDT1 = extract_elds_date(eldstxt_path, "ELDSTXT")
    ELDSDT2 = extract_elds_date(eldstx2_path, "ELDSTX2")
    
    print(f"ELDSDT1: {ELDSDT1}")
    print(f"ELDSDT2: {ELDSDT2}")
    
    # 5. Process BRH
    print("\n" + "-" * 60)
    brh_df = process_brh(brh_path)
    
    # 6. Process ELN1 and ELN2
    print("\n" + "-" * 60)
    eln1_df = process_eln1(eldstxt_path)
    
    print("\n" + "-" * 60)
    eln2_df = process_eln2(eldstx2_path)
    
    if eln1_df.is_empty() or eln2_df.is_empty():
        print("ERROR: No data processed from ELN1 or ELN2")
        return None
    
    # 7. Merge ELN1 and ELN2
    print("\n" + "-" * 60)
    print("Merging datasets...")
    
    # Sort by AANO, STATUS
    eln1_sorted = eln1_df.sort(['AANO', 'STATUS'])
    eln2_sorted = eln2_df.sort(['AANO', 'STATUS'])
    
    # Merge on AANO and STATUS
    sibc_df = eln1_sorted.join(eln2_sorted, on=['AANO', 'STATUS'], how='outer')
    print(f"After ELN1+ELN2 merge: {len(sibc_df)} records")
    
    # 8. Apply SAS conditional logic
    print("Applying business logic (IF STATUS='APPLIED' AND AMOUNT missing/0 THEN AMOUNT=AMOUNTX)...")
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
    
    # Remove duplicates (NODUPKEY equivalent)
    sibc_df = sibc_df.unique(subset=['AANO', 'STATUS'])
    print(f"After deduplication: {len(sibc_df)} records")
    
    # 9. Merge with BRH data
    print("\n" + "-" * 60)
    print("Merging with branch data...")
    
    if not brh_df.is_empty():
        sibc_df = sibc_df.join(brh_df, on='BRCD', how='inner')
        print(f"After BRH merge: {len(sibc_df)} records")
    else:
        print("Warning: No BRH data available")
    
    # Drop BRCD column as in SAS
    if 'BRCD' in sibc_df.columns:
        sibc_df = sibc_df.drop('BRCD')
    
    # 10. Save output
    print("\n" + "-" * 60)
    print("Saving output files...")
    
    output_name = f"SIBC{REPTMON}{REPTYEAR}{NOWK}"
    
    # Create output directory (using current script location)
    script_dir = Path(__file__).parent
    output_dir = script_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save to parquet (main output)
    parquet_path = output_dir / f"{output_name}.parquet"
    sibc_df.write_parquet(parquet_path)
    print(f"✓ Saved Parquet file: {parquet_path}")
    
    # Save to CSV
    csv_path = output_dir / f"{output_name}.csv"
    sibc_df.write_csv(csv_path)
    print(f"✓ Saved CSV file: {csv_path}")
    
    # 11. Create a summary report
    summary_path = output_dir / f"{output_name}_SUMMARY.txt"
    with open(summary_path, 'w') as f:
        f.write(f"EIBWSIBC Processing Summary\n")
        f.write(f"{'=' * 40}\n")
        f.write(f"Processing Date: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Report Period: {REPTMON}/{REPTYEAR}, Week: {NOWK}\n")
        f.write(f"ELDSDT1: {ELDSDT1}\n")
        f.write(f"ELDSDT2: {ELDSDT2}\n")
        f.write(f"Final Records: {len(sibc_df)}\n")
        f.write(f"Output Files:\n")
        f.write(f"  - {parquet_path}\n")
        f.write(f"  - {csv_path}\n")
    
    print(f"✓ Saved summary file: {summary_path}")
    
    return sibc_df

# ------------------------------------------------------------
# 7. EXECUTE
# ------------------------------------------------------------
if __name__ == "__main__":
    try:
        result = process_loan_reports()
        
        if result is not None:
            print("\n" + "=" * 60)
            print("PROCESSING COMPLETE!")
            print("=" * 60)
            print(f"\nFinal dataset has {len(result)} records")
            print("\nFirst 5 records:")
            print(result.head(5))
            print(f"\nColumns: {result.columns}")
        else:
            print("\nProcessing failed. Please check error messages above.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\nProcessing interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
