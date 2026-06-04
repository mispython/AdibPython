import polars as pl
import duckdb
from pathlib import Path
import datetime
from datetime import date
import re
import sys
import calendar

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
    """Extract date from ELDSTXT/ELDSTX2 first line"""
    try:
        if not Path(file_path).exists():
            print(f"Warning: File not found: {file_path}")
            return None
            
        with open(file_path, 'r', encoding='latin-1') as f:
            line = f.readline()
            print(f"First line of {file_name}: {line[:100]}...")
            
            if len(line) >= 62:
                date_patterns = [
                    r'(\d{2})/(\d{2})/(\d{4})',
                    r'(\d{2})-(\d{2})-(\d{4})',
                    r'(\d{2})\.(\d{2})\.(\d{4})'
                ]
                
                for pattern in date_patterns:
                    date_match = re.search(pattern, line)
                    if date_match:
                        dd, mm, yy = date_match.groups()
                        return date(int(yy), int(mm), int(dd))
                
                try:
                    if len(line) >= 62:
                        dd = int(line[52:54])
                        mm = int(line[55:57])
                        yy = int(line[58:62])
                        return date(yy, mm, dd)
                except ValueError as e:
                    print(f"Warning: Could not parse fixed position date in {file_name}: {e}")
                    
    except Exception as e:
        print(f"Warning: Could not extract date from {file_path}: {e}")
    
    return None

# ------------------------------------------------------------
# 4. PROCESS ELN1 (From ELDSTXT) - COMPLETE COLUMNS
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
        
        for line_num, line in enumerate(all_lines[1:], 2):
            line = line.rstrip('\n')
            if len(line.strip()) == 0:
                continue
            
            # Parse all fields based on positions from sample
            aano = line[0:13].strip() if len(line) >= 13 else ''
            faccode = line[13:21].strip() if len(line) >= 21 else ''
            facili = line[21:31].strip() if len(line) >= 31 else ''
            bnmeff = line[31:36].strip() if len(line) >= 36 else ''
            appric = line[36:51].strip() if len(line) >= 51 else ''
            amtapply_str = line[51:66].strip() if len(line) >= 66 else ''
            avpric = line[66:81].strip() if len(line) >= 81 else ''
            pricing = line[81:96].strip() if len(line) >= 96 else ''
            newic = line[96:106].strip() if len(line) >= 106 else ''
            cparty = line[106:116].strip() if len(line) >= 116 else ''
            lntype = line[116:126].strip() if len(line) >= 126 else ''
            gincome_str = line[126:141].strip() if len(line) >= 141 else ''
            spaamt_str = line[141:156].strip() if len(line) >= 156 else ''
            cprelat = line[156:158].strip() if len(line) >= 158 else ''
            cprelas = line[158:160].strip() if len(line) >= 160 else ''
            cpstaff = line[160:162].strip() if len(line) >= 162 else ''
            cpditor = line[162:164].strip() if len(line) >= 164 else ''
            cpstfid = line[164:166].strip() if len(line) >= 166 else ''
            cpbrho = line[166:168].strip() if len(line) >= 168 else ''
            status = line[168:180].strip().upper() if len(line) >= 180 else ''
            felimit_str = line[180:195].strip() if len(line) >= 195 else ''
            trlimit_str = line[195:210].strip() if len(line) >= 210 else ''
            custcode = line[210:212].strip() if len(line) >= 212 else ''
            sector = line[212:216].strip() if len(line) >= 216 else ''
            pcodcris = line[216:220].strip() if len(line) >= 220 else ''
            pcodfiss = line[220:224].strip() if len(line) >= 224 else ''
            smesize = line[224:226].strip() if len(line) >= 226 else ''
            noempl_str = line[226:233].strip() if len(line) >= 233 else ''
            turnover_str = line[233:248].strip() if len(line) >= 248 else ''
            apvby = line[248:250].strip() if len(line) >= 250 else ''
            apvby2 = line[250:252].strip() if len(line) >= 252 else ''
            apvdes1 = line[252:262].strip() if len(line) >= 262 else ''
            apvdes2 = line[262:272].strip() if len(line) >= 272 else ''
            reasons = line[272:282].strip() if len(line) >= 282 else ''
            icreason = line[282:292].strip() if len(line) >= 292 else ''
            smename1 = line[292:312].strip() if len(line) >= 312 else ''
            smename2 = line[312:332].strip() if len(line) >= 332 else ''
            tranbr = line[332:334].strip() if len(line) >= 334 else ''
            tranbrno = line[334:336].strip() if len(line) >= 336 else ''
            tranreg = line[336:338].strip() if len(line) >= 338 else ''
            advances_str = line[338:353].strip() if len(line) >= 353 else ''
            product = line[353:363].strip() if len(line) >= 363 else ''
            state = line[363:365].strip() if len(line) >= 365 else ''
            exstlmt_str = line[365:380].strip() if len(line) >= 380 else ''
            greentco = line[380:382].strip() if len(line) >= 382 else ''
            biotco = line[382:384].strip() if len(line) >= 384 else ''
            smeip = line[384:386].strip() if len(line) >= 386 else ''
            sme1incr = line[386:388].strip() if len(line) >= 388 else ''
            smemsc = line[388:390].strip() if len(line) >= 390 else ''
            strupco_2yr = line[390:392].strip() if len(line) >= 392 else ''
            ctry_incorp = line[392:402].strip() if len(line) >= 402 else ''
            strupco_3yr = line[402:404].strip() if len(line) >= 404 else ''
            name = line[404:444].strip() if len(line) >= 444 else ''
            ln_utilise_locat_cd = line[444:446].strip() if len(line) >= 446 else ''
            new_buss_reg_id = line[446:448].strip() if len(line) >= 448 else ''
            climate_prin_taxonomy_class = line[448:450].strip() if len(line) >= 450 else ''
            source_income_currency_cd = line[450:453].strip() if len(line) >= 453 else ''
            grp_annl_sales_financial_dt = line[453:461].strip() if len(line) >= 461 else ''
            grp_annl_sales_amt_str = line[461:476].strip() if len(line) >= 476 else ''
            
            # Parse numeric fields
            def parse_numeric(val_str):
                if val_str:
                    try:
                        return float(val_str.replace(',', '').replace(' ', ''))
                    except:
                        return None
                return None
            
            amtapply = parse_numeric(amtapply_str)
            gincome = parse_numeric(gincome_str)
            spaamt = parse_numeric(spaamt_str)
            felimit = parse_numeric(felimit_str)
            trlimit = parse_numeric(trlimit_str)
            noempl = parse_numeric(noempl_str)
            turnover = parse_numeric(turnover_str)
            advances = parse_numeric(advances_str)
            exstlmt = parse_numeric(exstlmt_str)
            grp_annl_sales_amt = parse_numeric(grp_annl_sales_amt_str)
            
            lines.append({
                'AANO': aano,
                'FACCODE': faccode,
                'FACILI': facili,
                'BNMEFF': bnmeff,
                'APPRIC': appric,
                'AMTAPPLY': amtapply,
                'AVPRIC': avpric,
                'PRICING': pricing,
                'NEWIC': newic,
                'CPARTY': cparty,
                'LNTYPE': lntype,
                'GINCOME': gincome,
                'SPAAMT': spaamt,
                'CPRELAT': cprelat,
                'CPRELAS': cprelas,
                'CPSTAFF': cpstaff,
                'CPDITOR': cpditor,
                'CPSTFID': cpstfid,
                'CPBRHO': cpbrho,
                'STATUS': status,
                'FELIMIT': felimit,
                'TRLIMIT': trlimit,
                'CUSTCODE': custcode,
                'SECTOR': sector,
                'PCODCRIS': pcodcris,
                'PCODFISS': pcodfiss,
                'SMESIZE': smesize,
                'NOEMPL': noempl,
                'TURNOVER': turnover,
                'APVBY': apvby,
                'APVBY2': apvby2,
                'APVDES1': apvdes1,
                'APVDES2': apvdes2,
                'REASONS': reasons,
                'ICREASON': icreason,
                'SMENAME1': smename1,
                'SMENAME2': smename2,
                'TRANBR': tranbr,
                'TRANBRNO': tranbrno,
                'TRANREG': tranreg,
                'ADVANCES': advances,
                'PRODUCT': product,
                'STATE': state,
                'EXSTLMT': exstlmt,
                'GREENTCO': greentco,
                'BIOTCO': biotco,
                'SMEIP': smeip,
                'SME1INCR': sme1incr,
                'SMEMSC': smemsc,
                'STRUPCO_2YR': strupco_2yr,
                'CTRY_INCORP': ctry_incorp,
                'STRUPCO_3YR': strupco_3yr,
                'NAME': name,
                'LN_UTILISE_LOCAT_CD': ln_utilise_locat_cd,
                'NEW_BUSS_REG_ID': new_buss_reg_id,
                'CLIMATE_PRIN_TAXONOMY_CLASS': climate_prin_taxonomy_class,
                'SOURCE_INCOME_CURRENCY_CD': source_income_currency_cd,
                'GRP_ANNL_SALES_FINANCIAL_DT': grp_annl_sales_financial_dt,
                'GRP_ANNL_SALES_AMT': grp_annl_sales_amt,
                'AMOUNT': amtapply  # Initialize AMOUNT with AMTAPPLY
            })
            
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
                        return float(val_str.replace(',', '').replace(' ', ''))
                    except:
                        return None
                return None
            
            amount = parse_numeric(amount_str)
            
            # Parse dates
            def parse_date(date_str):
                if date_str and len(date_str) >= 6:
                    try:
                        # Format: DDMMYY or YYMMDD?
                        # Based on sample, looks like DDMMYY
                        if len(date_str) == 6:
                            dd = int(date_str[0:2])
                            mm = int(date_str[2:4])
                            yy = int(date_str[4:6])
                            # Assuming 20xx for year
                            year = 2000 + yy if yy < 70 else 1900 + yy
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
    
    # Define file paths
    eldstxt_path = Path("/stgsrcsys/host/uat/ELDSTXT.txt")
    eldstx2_path = Path("/stgsrcsys/host/uat/ELDSTX2.txt")
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
    ELDSDT1 = extract_elds_date(eldstxt_path, "ELDSTXT")
    ELDSDT2 = extract_elds_date(eldstx2_path, "ELDSTX2")
    
    print(f"ELDSDT1: {ELDSDT1}")
    print(f"ELDSDT2: {ELDSDT2}")
    
    # Process BRH
    print("\n" + "-" * 60)
    brh_df = process_brh(brh_path)
    
    # Process ELN1 and ELN2
    print("\n" + "-" * 60)
    eln1_df = process_eln1(eldstxt_path)
    
    print("\n" + "-" * 60)
    eln2_df = process_eln2(eldstx2_path)
    
    if eln1_df.is_empty() or eln2_df.is_empty():
        print("ERROR: No data processed from ELN1 or ELN2")
        return None
    
    # Merge ELN1 and ELN2
    print("\n" + "-" * 60)
    print("Merging datasets...")
    
    # Merge on AANO and STATUS
    sibc_df = eln1_df.join(eln2_df, on=['AANO', 'STATUS'], how='outer')
    print(f"After ELN1+ELN2 merge: {len(sibc_df)} records")
    
    # Apply business logic: If AMOUNT is missing or 0 from ELN2, use AMTAPPLY from ELN1
    # But keep AMOUNT from ELN2 as primary source
    sibc_df = sibc_df.with_columns([
        pl.when(
            (pl.col('AMOUNT').is_null()) | (pl.col('AMOUNT') == 0)
        )
        .then(pl.col('AMTAPPLY'))
        .otherwise(pl.col('AMOUNT'))
        .alias('AMOUNT')
    ])
    
    # Remove duplicates
    sibc_df = sibc_df.unique(subset=['AANO', 'STATUS'])
    print(f"After deduplication: {len(sibc_df)} records")
    
    # Merge with BRH data if needed (BRANCH column from ELN2 might already have branch info)
    # Only merge if BRANCH column is missing or needs mapping
    if not brh_df.is_empty() and 'BRANCH' in sibc_df.columns:
        # If BRANCH exists but we need to map from BRCD, we would do that here
        pass
    
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
    
    # Save to CSV (matching sample format)
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
        f.write(f"ELDSDT1: {ELDSDT1}\n")
        f.write(f"ELDSDT2: {ELDSDT2}\n")
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
            print(f"\nColumns ({len(result.columns)}): {result.columns[:20]}...")  # Show first 20 columns
            print("\nSample data:")
            print(result.head(5))
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
