import polars as pl
import duckdb
from pathlib import Path
import datetime
from datetime import date
import re
import sys
import calendar
import pyreadstat  # Better library for reading SAS files
import logging

# Suppress unnecessary logging
logging.basicConfig(level=logging.WARNING)

# ------------------------------------------------------------
# 1. REPTDATE CALCULATION
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
# 2. BRH PROCESSING
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
                    
                brstat = line[-1] if len(line) > 0 else ''
                
                if brstat != 'C':
                    parts = line.split()
                    if len(parts) >= 2:
                        branch = parts[0]
                        brcd = parts[1]
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
    """Extract date from ELDSTX2 first line"""
    try:
        if not Path(file_path).exists():
            print(f"Warning: File not found: {file_path}")
            return None
            
        with open(file_path, 'r', encoding='latin-1') as f:
            line = f.readline()
            print(f"First line of {file_name}: {line[:100]}...")
            
            # Look for date pattern in the line
            date_patterns = [
                r'(\d{2})-(\d{2})-(\d{4})',
                r'(\d{2})/(\d{2})/(\d{4})',
                r'(\d{2})\.(\d{2})\.(\d{4})',
                r'(\d{2})(\d{2})(\d{4})'
            ]
            
            for pattern in date_patterns:
                date_match = re.search(pattern, line)
                if date_match:
                    dd, mm, yy = date_match.groups()
                    return date(int(yy), int(mm), int(dd))
                    
    except Exception as e:
        print(f"Warning: Could not extract date from {file_path}: {e}")
    
    return None

def extract_elds_date_from_sas(sas_file_path, file_name):
    """Extract date from SAS dataset metadata"""
    try:
        sas_path_str = str(sas_file_path)  # Convert Path to string
        if not Path(sas_path_str).exists():
            print(f"Warning: SAS file not found: {sas_path_str}")
            return None
        
        # Read only metadata to get file creation/modification info
        df, meta = pyreadstat.read_sas7bdat(sas_path_str, rows_limit=1)
        
        # Try to get date from metadata
        if hasattr(meta, 'table_date'):
            if meta.table_date:
                print(f"Extracted date from SAS metadata: {meta.table_date}")
                return meta.table_date.date() if hasattr(meta.table_date, 'date') else meta.table_date
        
        # If no date in metadata, use file modification time
        mod_time = Path(sas_path_str).stat().st_mtime
        file_date = datetime.datetime.fromtimestamp(mod_time).date()
        print(f"Using file modification date: {file_date}")
        return file_date
            
    except Exception as e:
        print(f"Warning: Could not extract date from SAS file {sas_file_path}: {e}")
        import traceback
        traceback.print_exc()
    
    return None

# ------------------------------------------------------------
# 4. PROCESS ELN1 (From SAS Dataset) - COMPLETE COLUMNS
# ------------------------------------------------------------
def process_eln1_from_sas(file_path):
    """Process ELN1 from SAS dataset (.sas7bdat) using pyreadstat"""
    print(f"Processing ELN1 from SAS dataset: {file_path}")
    
    try:
        # Convert Path to string
        sas_path_str = str(file_path)
        
        # Read SAS dataset directly into pandas then convert to polars
        # pyreadstat is more robust than sas7bdat
        print(f"Reading SAS file: {sas_path_str}")
        df_pd, meta = pyreadstat.read_sas7bdat(sas_path_str)
        
        print(f"Successfully read SAS dataset with {len(df_pd)} rows and {len(df_pd.columns)} columns")
        print(f"Column names: {list(df_pd.columns[:20])}...")  # Show first 20 columns
        
        # Convert to polars DataFrame
        df = pl.from_pandas(df_pd)
        
        # Standardize column names to uppercase to match expected format
        df = df.rename({col: col.upper() for col in df.columns})
        
        # Ensure all expected columns exist (create missing ones as null)
        expected_columns = [
            'AANO', 'FACCODE', 'FACILI', 'BNMEFF', 'APPRIC', 'AMTAPPLY', 'AVPRIC', 'PRICING',
            'NEWIC', 'CPARTY', 'LNTYPE', 'GINCOME', 'SPAAMT', 'CPRELAT', 'CPRELAS', 'CPSTAFF',
            'CPDITOR', 'CPSTFID', 'CPBRHO', 'STATUS', 'FELIMIT', 'TRLIMIT', 'CUSTCODE', 'SECTOR',
            'PCODCRIS', 'PCODFISS', 'SMESIZE', 'NOEMPL', 'TURNOVER', 'APVBY', 'APVBY2', 'APVDES1',
            'APVDES2', 'REASONS', 'ICREASON', 'SMENAME1', 'SMENAME2', 'TRANBR', 'TRANBRNO', 'TRANREG',
            'ADVANCES', 'PRODUCT', 'STATE', 'EXSTLMT', 'GREENTCO', 'BIOTCO', 'SMEIP', 'SME1INCR',
            'SMEMSC', 'STRUPCO_2YR', 'CTRY_INCORP', 'STRUPCO_3YR', 'NAME', 'LN_UTILISE_LOCAT_CD',
            'NEW_BUSS_REG_ID', 'CLIMATE_PRIN_TAXONOMY_CLASS', 'SOURCE_INCOME_CURRENCY_CD',
            'GRP_ANNL_SALES_FINANCIAL_DT', 'GRP_ANNL_SALES_AMT'
        ]
        
        # Add missing columns with null values
        for col in expected_columns:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))
        
        # Ensure AMOUNT column is initialized with AMTAPPLY if available
        if 'AMOUNT' not in df.columns and 'AMTAPPLY' in df.columns:
            df = df.with_columns(pl.col('AMTAPPLY').alias('AMOUNT'))
        elif 'AMOUNT' in df.columns and 'AMTAPPLY' in df.columns:
            # Keep AMOUNT as is, but ensure it exists
            pass
        else:
            df = df.with_columns(pl.lit(None).alias('AMOUNT'))
        
        # Clean numeric fields - convert to float where possible
        numeric_fields = ['AMTAPPLY', 'GINCOME', 'SPAAMT', 'FELIMIT', 'TRLIMIT', 
                         'NOEMPL', 'TURNOVER', 'ADVANCES', 'EXSTLMT', 'GRP_ANNL_SALES_AMT', 'AMOUNT']
        
        for field in numeric_fields:
            if field in df.columns:
                try:
                    # Try to cast to float, replace errors with null
                    df = df.with_columns(
                        pl.col(field).cast(pl.Float64, strict=False).alias(field)
                    )
                except Exception as e:
                    print(f"Warning: Could not convert {field} to float: {e}")
        
        print(f"ELN1 records processed from SAS: {len(df)}")
        
        # Show first few rows for verification
        if len(df) > 0:
            print("\nFirst 2 rows of ELN1 data:")
            print(df.head(2))
        
        return df
        
    except FileNotFoundError:
        print(f"ERROR: SAS file not found at {file_path}")
        return pl.DataFrame()
    except Exception as e:
        print(f"ERROR processing ELN1 from SAS: {e}")
        import traceback
        traceback.print_exc()
        return pl.DataFrame()

# ------------------------------------------------------------
# 5. PROCESS ELN2 (From ELDSTX2) - COMPLETE COLUMNS
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
            
            # Parse additional fields from ELDSTX2
            aano = line[0:13].strip() if len(line) >= 13 else ''
            status = line[13:25].strip().upper() if len(line) >= 25 else ''
            amount_str = line[25:40].strip() if len(line) >= 40 else ''
            aadate_str = line[40:48].strip() if len(line) >= 48 else ''
            sbdate_str = line[48:56].strip() if len(line) >= 56 else ''
            dpdate_str = line[56:64].strip() if len(line) >= 64 else ''
            iddate_str = line[64:72].strip() if len(line) >= 72 else ''
            lodate_str = line[72:80].strip() if len(line) >= 80 else ''
            cmdate_str = line[80:88].strip() if len(line) >= 88 else ''
            apvdte1_str = line[88:96].strip() if len(line) >= 96 else ''
            apvdte2_str = line[96:104].strip() if len(line) >= 104 else ''
            br_full_doc_receive_dt_str = line[104:112].strip() if len(line) >= 112 else ''
            hoe_full_doc_receive_dt_str = line[112:120].strip() if len(line) >= 120 else ''
            branch = line[120:123].strip() if len(line) >= 123 else ''
            
            # Parse numeric
            def parse_numeric(val_str):
                if val_str:
                    try:
                        # Remove any non-numeric characters except decimal point and minus sign
                        clean_str = re.sub(r'[^\d\-\.]', '', val_str)
                        if clean_str and clean_str != '-':
                            return float(clean_str)
                    except:
                        pass
                return None
            
            amount = parse_numeric(amount_str)
            
            # Parse dates (format: DDMMYY)
            def parse_date(date_str):
                if date_str and len(date_str) >= 6:
                    try:
                        dd = int(date_str[0:2])
                        mm = int(date_str[2:4])
                        yy = int(date_str[4:6])
                        # Assuming 20xx for years 00-69, 19xx for 70-99
                        year = 2000 + yy if yy <= 69 else 1900 + yy
                        return date(year, mm, dd)
                    except:
                        pass
                return None
            
            lines.append({
                'AANO': aano,
                'STATUS': status,
                'AMOUNT': amount,
                'AADATE': parse_date(aadate_str),
                'SBDATE': parse_date(sbdate_str),
                'DPDATE': parse_date(dpdate_str),
                'IDDATE': parse_date(iddate_str),
                'LODATE': parse_date(lodate_str),
                'CMDATE': parse_date(cmdate_str),
                'APVDTE1': parse_date(apvdte1_str),
                'APVDTE2': parse_date(apvdte2_str),
                'BR_FULL_DOC_RECEIVE_DT': parse_date(br_full_doc_receive_dt_str),
                'HOE_FULL_DOC_RECEIVE_DT': parse_date(hoe_full_doc_receive_dt_str),
                'BRANCH': branch
            })
            
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
    
    print("=" * 60)
    print("Starting EIBWSIBC Report Processing")
    print("=" * 60)
    
    macros = get_reptdate()
    NOWK = macros['NOWK']
    REPTMON = macros['REPTMON']
    REPTYEAR = macros['REPTYEAR']
    
    print(f"Processing for period: {REPTMON}/{REPTYEAR}, Week: {NOWK}")
    
    # Define file paths - ELDSTXT is SAS dataset
    eldstxt_path = Path("/stgsrcsys/host/uat/sibc05264.sas7bdat")  # Your SAS file
    eldstx2_path = Path("/stgsrcsys/host/uat/BNMSIBC2.TXT")
    brh_path = Path("/sasdata/rawdata/lookup/LKP_BRANCH")
    
    # Check if input files exist
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
    
    # Extract dates from ELDS files
    print("\n" + "-" * 60)
    print("Extracting ELDS dates...")
    ELDSDT1 = extract_elds_date_from_sas(eldstxt_path, "ELDSTXT (SAS)")
    ELDSDT2 = extract_elds_date(eldstx2_path, "ELDSTX2")
    
    print(f"ELDSDT1 (from SAS): {ELDSDT1}")
    print(f"ELDSDT2: {ELDSDT2}")
    
    # Process BRH
    print("\n" + "-" * 60)
    brh_df = process_brh(brh_path)
    
    # Process ELN1 from SAS and ELN2 from text
    print("\n" + "-" * 60)
    eln1_df = process_eln1_from_sas(eldstxt_path)
    
    print("\n" + "-" * 60)
    eln2_df = process_eln2(eldstx2_path)
    
    if eln1_df.is_empty():
        print("ERROR: No data processed from ELN1 (SAS file)")
        return None
    
    if eln2_df.is_empty():
        print("ERROR: No data processed from ELN2 (text file)")
        return None
    
    # Merge ELN1 and ELN2
    print("\n" + "-" * 60)
    print("Merging datasets...")
    
    # Ensure columns exist for merging
    if 'AANO' not in eln1_df.columns:
        print("ERROR: 'AANO' column missing from ELN1 data")
        return None
    
    if 'STATUS' not in eln1_df.columns:
        # Add STATUS column if missing
        eln1_df = eln1_df.with_columns(pl.lit(None).alias('STATUS'))
    
    # Merge on AANO and STATUS
    sibc_df = eln1_df.join(eln2_df, on=['AANO', 'STATUS'], how='left')
    print(f"After ELN1+ELN2 merge: {len(sibc_df)} records")
    
    # Apply business logic: If AMOUNT is missing or 0 from ELN2, use AMTAPPLY from ELN1
    if 'AMOUNT' in sibc_df.columns and 'AMTAPPLY' in sibc_df.columns:
        sibc_df = sibc_df.with_columns([
            pl.when(
                (pl.col('AMOUNT').is_null()) | (pl.col('AMOUNT') == 0)
            )
            .then(pl.col('AMTAPPLY'))
            .otherwise(pl.col('AMOUNT'))
            .alias('AMOUNT')
        ])
    
    # Remove duplicates
    if 'AANO' in sibc_df.columns:
        sibc_df = sibc_df.unique(subset=['AANO', 'STATUS'] if 'STATUS' in sibc_df.columns else ['AANO'])
        print(f"After deduplication: {len(sibc_df)} records")
    
    # Reorder columns to match sample output
    column_order = [
        'AANO', 'FACCODE', 'FACILI', 'BNMEFF', 'APPRIC', 'AMTAPPLY', 'AVPRIC', 'PRICING',
        'NEWIC', 'CPARTY', 'LNTYPE', 'GINCOME', 'SPAAMT', 'CPRELAT', 'CPRELAS', 'CPSTAFF',
        'CPDITOR', 'CPSTFID', 'CPBRHO', 'STATUS', 'FELIMIT', 'TRLIMIT', 'CUSTCODE', 'SECTOR',
        'PCODCRIS', 'PCODFISS', 'SMESIZE', 'NOEMPL', 'TURNOVER', 'APVBY', 'APVBY2', 'APVDES1',
        'APVDES2', 'REASONS', 'ICREASON', 'SMENAME1', 'SMENAME2', 'TRANBR', 'TRANBRNO', 'TRANREG',
        'ADVANCES', 'PRODUCT', 'STATE', 'EXSTLMT', 'GREENTCO', 'BIOTCO', 'SMEIP', 'SME1INCR',
        'SMEMSC', 'STRUPCO_2YR', 'CTRY_INCORP', 'STRUPCO_3YR', 'NAME', 'LN_UTILISE_LOCAT_CD',
        'NEW_BUSS_REG_ID', 'CLIMATE_PRIN_TAXONOMY_CLASS', 'SOURCE_INCOME_CURRENCY_CD',
        'GRP_ANNL_SALES_FINANCIAL_DT', 'GRP_ANNL_SALES_AMT', 'AMOUNT', 'AADATE', 'SBDATE',
        'DPDATE', 'IDDATE', 'LODATE', 'CMDATE', 'APVDTE1', 'APVDTE2', 'BR_FULL_DOC_RECEIVE_DT',
        'HOE_FULL_DOC_RECEIVE_DT', 'BRANCH'
    ]
    
    # Only keep columns that exist
    existing_cols = [col for col in column_order if col in sibc_df.columns]
    sibc_df = sibc_df.select(existing_cols)
    
    # Save output
    print("\n" + "-" * 60)
    print("Saving output files...")
    
    output_name = f"SIBC{REPTMON}{REPTYEAR}{NOWK}"
    
    script_dir = Path(__file__).parent
    output_dir = script_dir / "Output"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save to CSV
    csv_path = output_dir / f"{output_name}.csv"
    sibc_df.write_csv(csv_path)
    print(f"✓ Saved CSV file: {csv_path}")
    
    # Save to Parquet
    parquet_path = output_dir / f"{output_name}.parquet"
    sibc_df.write_parquet(parquet_path)
    print(f"✓ Saved Parquet file: {parquet_path}")
    
    # Create summary report
    summary_path = output_dir / f"{output_name}_SUMMARY.txt"
    with open(summary_path, 'w') as f:
        f.write(f"EIBWSIBC Processing Summary\n")
        f.write(f"{'=' * 40}\n")
        f.write(f"Processing Date: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Report Period: {REPTMON}/{REPTYEAR}, Week: {NOWK}\n")
        f.write(f"ELDSDT1 (from SAS): {ELDSDT1}\n")
        f.write(f"ELDSDT2: {ELDSDT2}\n")
        f.write(f"BRH Records: {len(brh_df)}\n")
        f.write(f"ELN1 Records: {len(eln1_df)}\n")
        f.write(f"ELN2 Records: {len(eln2_df)}\n")
        f.write(f"Final Records: {len(sibc_df)}\n")
        f.write(f"Output Files:\n")
        f.write(f"  - {csv_path}\n")
        f.write(f"  - {parquet_path}\n")
    
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
            print(f"\nColumns ({len(result.columns)}): {list(result.columns[:20])}...")
            print("\nSample data (first 5 rows):")
            print(result.head(5))
            
            # Show data types for verification
            print("\nData types of first 10 columns:")
            for col in result.columns[:10]:
                print(f"  {col}: {result[col].dtype}")
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
