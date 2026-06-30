# eibdcitx.py - FIXED DCI BUILDING
import polars as pl
from datetime import date, datetime, timedelta
import pyreadstat
import os
from pathlib import Path

# ===================================================================
# PATH CONFIGURATION
# ===================================================================

INPUT_PATHS = {
    "DPFL": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/DPFL.txt",
    "EQFL": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/UTSASDCID_{yyyy}{mm}{dd}.txt",
    "CRA": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/DPCRATXT_{yyyy}{mm}{dd}",
    "EQRATE": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/eqrate{yy}{mm}{dd}.sas7bdat",
    "MNITB_SAVING": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/intg_dp_acct_saving.sas7bdat",
    "MNITB_CURRENT": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/intg_dp_acct_current.sas7bdat",
    "DCID": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/dcid{mm}{dd}.sas7bdat",
}

PARQUET_CACHE = {
    "MNITB_SAVING": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/cache/intg_dp_acct_saving.parquet",
    "MNITB_CURRENT": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/cache/intg_dp_acct_current.parquet",
}

OUTPUT_PATHS = {
    "PARQUET": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/DCI_{date}.parquet",
    "CSV": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/DCI_{date}.csv",
    "SAS": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/BNMK_DCI{mon}{wk}.sas7bdat",
    "TEXT": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/DCITXT.txt",
}

EQC_KEEP_COLS = ['TICKETNO', 'CUSTICKETNO', 'BRANCH', 'INVCURR', 'ALTCURR',
                 'INVAMT', 'ALTAMT', 'TENOR', 'STATUSIND', 'DCIRT', 'STARTDT',
                 'MATDT', 'PREMPAID', 'TYPE']

EQI_KEEP_COLS = ['TICKETNO', 'CUSTNAME', 'CUSTRES', 'CUSTLOC', 'FISSCODE',
                 'CUSTICKETNO', 'BRANCH', 'INVCURR', 'ALTCURR', 'EQCUSTYP',
                 'INVAMT', 'ALTAMT', 'TENOR', 'STATUSIND', 'STARTDT',
                 'MATDT', 'PREMREC', 'TYPE']

BNM_CODES = {
    "ACCINTRM": "4911095000000Y",
    "PREMIUM": "4929996000000Y"
}

REPORT_CONFIG = {
    "MIN_CUSTCODE": 80,
    "VALID_STATUSES": ["ACT", "CEP", "CEU", "CCU", "CMU"],
    "JPY_CURRENCY": "JPY",
    "MYR_CURRENCY": "MYR",
    "DECIMAL_PLACES_JPY": 0,
    "DECIMAL_PLACES_OTHER": 2
}

ELDAY_MAPPING = {
    1: 'DAYA', 9: 'DAYA', 16: 'DAYA', 23: 'DAYA',
    2: 'DAYB', 10: 'DAYB', 17: 'DAYB', 24: 'DAYB',
    3: 'DAYC', 11: 'DAYC', 18: 'DAYC', 25: 'DAYC',
    4: 'DAYD', 12: 'DAYD', 19: 'DAYD', 26: 'DAYD',
    5: 'DAYE', 13: 'DAYE', 20: 'DAYE', 27: 'DAYE',
    6: 'DAYF', 14: 'DAYF', 21: 'DAYF', 28: 'DAYF',
    7: 'DAYG', 29: 'DAYG',
    30: 'DAYH',
    8: 'DAYI', 15: 'DAYI', 22: 'DAYI', 31: 'DAYI'
}

# ===================================================================
# Helper Functions
# ===================================================================

def ensure_directory(path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)

def format_path_with_date(path, date_vars):
    result = path
    for key, value in date_vars.items():
        result = result.replace(f'{{{key}}}', str(value))
    return result

def get_input_path(file_key, date_vars):
    return format_path_with_date(INPUT_PATHS[file_key], date_vars)

def get_output_path(file_key, date_vars):
    return format_path_with_date(OUTPUT_PATHS[file_key], date_vars)

def decode_packed_decimal(data):
    """Decode packed decimal (COMP-3) data from SAS"""
    if not data or len(data) == 0:
        return 0
    
    value = 0
    sign = 1
    
    for i, byte_val in enumerate(data):
        if isinstance(byte_val, str):
            byte_val = ord(byte_val)
        
        if i == len(data) - 1:
            # Last byte contains sign
            digit1 = (byte_val >> 4) & 0x0F
            sign_byte = byte_val & 0x0F
            if sign_byte in (0x0D, 0x0B):
                sign = -1
            elif sign_byte == 0x0C:
                sign = 1
            value = value * 10 + digit1
        else:
            digit1 = (byte_val >> 4) & 0x0F
            digit2 = byte_val & 0x0F
            value = value * 10 + digit1
            value = value * 10 + digit2
    
    return value * sign

def parse_cra_record(record):
    """Parse a single CRA record based on SAS INPUT statement"""
    # @001 BRANCH $3. - bytes 0-2
    branch = record[0:3].decode('ascii', errors='ignore').strip()
    
    # @007 CUSTICKETNO $60. - bytes 6-65
    custicketno = record[6:66].decode('ascii', errors='ignore').strip()
    
    # @067 INVCURAC PD6. - bytes 66-71
    invcurac = decode_packed_decimal(record[66:72])
    
    # @073 CUSTNAME $140. - bytes 72-211
    custname = record[72:212].decode('ascii', errors='ignore').strip()
    
    # @443 INVAMT PD7.2 - bytes 442-448
    invamt = decode_packed_decimal(record[442:449]) / 100
    
    # @450 STARTDT YYMMDD10. - bytes 449-458
    startdt = record[449:459].decode('ascii', errors='ignore').strip()
    
    # @460 MATDT YYMMDD10. - bytes 459-468
    matdt = record[459:469].decode('ascii', errors='ignore').strip()
    
    # @477 DCIRT PD7.7 - bytes 476-482
    dcirt = decode_packed_decimal(record[476:483]) / 10000000
    
    # @486 TENOR PD2. - bytes 485-486
    tenor = decode_packed_decimal(record[485:487])
    
    # @488 INV_STATUS $3. - bytes 487-489 (THIS IS A STRING, NOT PACKED DECIMAL!)
    inv_status = record[487:490].decode('ascii', errors='ignore').strip()
    
    # @494 ACCINT PD8.6 - bytes 493-500
    accint = decode_packed_decimal(record[493:501]) / 1000000
    
    # @839 CUSTCODE_DB2 2. - bytes 838-839
    custcode_db2 = record[838:840].decode('ascii', errors='ignore').strip()
    
    return {
        'BRANCH': branch,
        'CUSTICKETNO': custicketno,
        'INVCURAC': invcurac,
        'CUSTNAME': custname,
        'INVAMT': invamt,
        'STARTDT': startdt,
        'MATDT': matdt,
        'DCIRT': dcirt,
        'TENOR': tenor,
        'INV_STATUS': inv_status,
        'ACCINT': accint,
        'CUSTCODE_DB2': int(custcode_db2) if custcode_db2 else None
    }

def load_cra_file(file_path):
    """Load CRA binary file with proper record structure"""
    records = []
    
    try:
        with open(file_path, 'rb') as f:
            data = f.read()
        
        file_size = len(data)
        record_length = 942
        num_records = file_size // record_length
        
        print(f"  File size: {file_size:,} bytes")
        print(f"  Record length: {record_length} bytes")
        print(f"  Found {num_records:,} records")
        
        for i in range(num_records):
            offset = i * record_length
            record = data[offset:offset+record_length]
            
            if len(record) < record_length:
                break
            
            try:
                row = parse_cra_record(record)
                records.append(row)
            except Exception as e:
                continue
        
        print(f"  Parsed {len(records):,} CRA records")
        return pl.DataFrame(records)
        
    except Exception as e:
        print(f"  Error loading CRA: {e}")
        raise

def load_fixed_width_file(file_path, widths, columns, dtypes=None, encoding='utf-8'):
    try:
        with open(file_path, 'r', encoding=encoding) as f:
            lines = f.readlines()
    except UnicodeDecodeError:
        with open(file_path, 'r', encoding='latin-1') as f:
            lines = f.readlines()
    except Exception:
        with open(file_path, 'rb') as f:
            content = f.read()
            text = content.decode('latin-1', errors='replace')
            lines = text.splitlines(keepends=True)
    
    data = []
    for line in lines:
        if line.strip():
            row = {}
            start = 0
            for i, width in enumerate(widths):
                field = line[start:start+width].strip()
                col_name = columns[i]
                if dtypes and col_name in dtypes:
                    dtype = dtypes[col_name]
                    if dtype == pl.Int64:
                        try:
                            row[col_name] = int(field) if field else None
                        except:
                            row[col_name] = None
                    elif dtype == pl.Float64:
                        try:
                            row[col_name] = float(field) if field else None
                        except:
                            row[col_name] = None
                    else:
                        row[col_name] = field
                else:
                    row[col_name] = field
                start += width
            data.append(row)
    return pl.DataFrame(data)

def load_sas_file_fast(file_path, columns_to_keep=None):
    try:
        try:
            df, meta = pyreadstat.read_sas7bdat(file_path, columns=columns_to_keep)
            return pl.DataFrame(df)
        except TypeError:
            print(f"  Note: pyreadstat doesn't support column selection, loading full file then filtering...")
            df, meta = pyreadstat.read_sas7bdat(file_path)
            pl_df = pl.DataFrame(df)
            if columns_to_keep:
                existing_cols = [col for col in columns_to_keep if col in pl_df.columns]
                if existing_cols:
                    return pl_df.select(existing_cols)
            return pl_df
    except Exception as e:
        print(f"Error loading SAS file {file_path}: {e}")
        raise

def load_mnitb_with_cache(file_path, cache_path, columns_to_keep=['ACCTNO', 'CUSTCODE']):
    ensure_directory(cache_path)
    if os.path.exists(cache_path):
        return pl.read_parquet(cache_path)
    else:
        print(f"  Converting SAS to Parquet (first time, this may take a moment)...")
        try:
            try:
                df, meta = pyreadstat.read_sas7bdat(file_path, columns=columns_to_keep)
                pl_df = pl.DataFrame(df)
            except TypeError:
                print(f"  Note: pyreadstat doesn't support column selection, loading full file then filtering...")
                df, meta = pyreadstat.read_sas7bdat(file_path)
                pl_df = pl.DataFrame(df)
                existing_cols = [col for col in columns_to_keep if col in pl_df.columns]
                if existing_cols:
                    pl_df = pl_df.select(existing_cols)
            pl_df.write_parquet(cache_path)
            print(f"  ✓ Converted and cached to Parquet: {cache_path}")
            return pl_df
        except Exception as e:
            print(f"Error loading SAS file {file_path}: {e}")
            raise

def load_eqfl_file(file_path, separator='|', columns=None, dtypes=None):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except UnicodeDecodeError:
        with open(file_path, 'r', encoding='latin-1') as f:
            lines = f.readlines()
    
    data = []
    for line in lines:
        line = line.strip()
        if line:
            fields = line.split(separator)
            if len(fields) > len(columns):
                fields = fields[:len(columns)]
            elif len(fields) < len(columns):
                fields.extend([None] * (len(columns) - len(fields)))
            
            row = {}
            for i, col_name in enumerate(columns):
                field = fields[i].strip() if i < len(fields) and fields[i] else ''
                if dtypes and col_name in dtypes:
                    dtype = dtypes[col_name]
                    if dtype == pl.Int64:
                        try:
                            row[col_name] = int(field) if field else None
                        except:
                            row[col_name] = None
                    elif dtype == pl.Float64:
                        try:
                            row[col_name] = float(field) if field else None
                        except:
                            row[col_name] = None
                    else:
                        row[col_name] = field
                else:
                    row[col_name] = field
            data.append(row)
    return pl.DataFrame(data)

def ensure_join_key_type(df, column_name, target_type=pl.Int64):
    if column_name in df.columns:
        return df.with_columns([pl.col(column_name).cast(target_type)])
    return df

def safe_concat(dfs):
    non_empty = [df for df in dfs if len(df) > 0]
    if not non_empty:
        return pl.DataFrame()
    if len(non_empty) == 1:
        return non_empty[0]
    common_cols = set(non_empty[0].columns)
    for df in non_empty[1:]:
        common_cols = common_cols.intersection(set(df.columns))
    common_cols = list(common_cols)
    aligned_dfs = [df.select(common_cols) for df in non_empty]
    return pl.concat(aligned_dfs)

def convert_sas_date_to_display(sas_date):
    if sas_date is None or sas_date == '' or sas_date == 0:
        return ''
    try:
        if isinstance(sas_date, str) and sas_date.isdigit():
            sas_date = float(sas_date)
        if isinstance(sas_date, (int, float)):
            base_date = datetime(1960, 1, 1)
            target_date = base_date + timedelta(days=int(float(sas_date)))
            return target_date.strftime("%d/%m/%y")
        return str(sas_date)
    except:
        return str(sas_date)

def format_date_for_display(val):
    if val is None or val == '':
        return ''
    try:
        if isinstance(val, str):
            if val.isdigit():
                return convert_sas_date_to_display(float(val))
            if '/' in val:
                return val
            return val
        if isinstance(val, (int, float)):
            return convert_sas_date_to_display(val)
        return str(val)
    except:
        return str(val)

def scale_value(val):
    if val is None or val == 0:
        return 0
    if abs(val) > 1000000:
        return val / 1000000
    return val

def write_sas_file(df, file_path):
    try:
        import saspy
        print("  Connecting to SAS session...")
        sas = saspy.SASsession()
        df_pd = df.to_pandas()
        filename = os.path.basename(file_path)
        dataset_name = filename.replace('.sas7bdat', '')
        print(f"  Writing SAS dataset: {dataset_name}...")
        sas.df2sd(df_pd, table=dataset_name, libref='user')
        sas.submit(f'''
            libname outlib "{os.path.dirname(file_path)}";
            proc copy in=user out=outlib;
                select {dataset_name};
            run;
        ''')
        print(f"  ✓ SAS dataset written: {file_path}")
        return True
    except Exception as e:
        print(f"  ✗ Could not write SAS file: {e}")
        csv_path = file_path.replace('.sas7bdat', '.csv')
        df.write_csv(csv_path)
        print(f"  ✓ CSV fallback written: {csv_path}")
        return False

# ===================================================================
# Main Processing
# ===================================================================

def main():
    yesterday = date.today() - timedelta(days=1)
    today = yesterday
    REPTDAY = f"{today.day:02d}"
    REPTMON = f"{today.month:02d}"
    REPTYEAR = f"{today.year % 100:02d}"
    REPTYEAR4 = f"{today.year:04d}"
    RDATE = today.strftime("%d/%m/%Y")

    day = today.day
    if 1 <= day <= 8:
        WK = "1"
    elif 9 <= day <= 15:
        WK = "2"
    elif 16 <= day <= 22:
        WK = "3"
    else:
        WK = "4"

    date_vars = {
        'yyyy': REPTYEAR4,
        'yy': REPTYEAR,
        'mm': REPTMON,
        'dd': REPTDAY,
        'date': f"{REPTYEAR}{REPTMON}{REPTDAY}",
        'day': REPTDAY,
        'mon': REPTMON,
        'year': REPTYEAR,
        'wk': WK,
        'rdate': RDATE
    }

    print(f"Running EIBDCITX for {RDATE} (WK={WK})")
    print("=" * 80)

    for output_path in OUTPUT_PATHS.values():
        ensure_directory(output_path)

    # -------------------------------------------------------------------
    # Load raw datasets
    # -------------------------------------------------------------------
    print("\nLoading input files...")
    
    try:
        dpfl_path = get_input_path("DPFL", date_vars)
        dpfl = load_fixed_width_file(
            dpfl_path,
            [7, 26, 20, 5, 11, 11, 15],
            ['TICKETNO', 'CUSTNAME', 'NEWIC', 'CUSTCODE', 
             'INVCURAC', 'ALTCURAC', 'ACCINT'],
            {'TICKETNO': pl.Utf8, 'CUSTNAME': pl.Utf8, 'NEWIC': pl.Utf8,
             'CUSTCODE': pl.Int64, 'INVCURAC': pl.Int64, 'ALTCURAC': pl.Int64,
             'ACCINT': pl.Float64}
        )
        print(f"  ✓ Loaded DPFL: {len(dpfl):,} rows")
    except Exception as e:
        print(f"  ✗ Error loading DPFL: {e}")
        return

    try:
        eqfl_path = get_input_path("EQFL", date_vars)
        eqfl = load_eqfl_file(
            eqfl_path,
            separator='|',
            columns=[
                'CUSTICKETNO', 'TICKETNO', 'BRANCH', 'CUSTNAME', 'DEALID',
                'FISSCODE', 'CUSTRES', 'CUSTMNE', 'CUSTLOC', 'EQCUSTYP',
                'PRODUCT', 'INVCURR', 'ALTCURR', 'INVAMT', 'INVAMTRM',
                'ALTAMT', 'TRADEDT', 'STARTDT', 'FIXINGDT', 'MATDT',
                'STOPDT', 'TENOR', 'STRIKERT', 'SPOTRT', 'DCIRT',
                'ACCINTAMT', 'TOTINTAMT', 'ACCINTRM', 'MMRT', 'RSPOTRT',
                'PREMREC', 'PREMPAID', 'PROFIT', 'PROFITMYR', 'UNWINDCOST',
                'STATUSIND', 'STATUS', 'TYPE'
            ],
            dtypes={'STARTDT': pl.Utf8, 'MATDT': pl.Utf8, 'ACCINTRM': pl.Float64,
                    'ACCINTAMT': pl.Float64, 'TOTINTAMT': pl.Float64,
                    'PREMPAID': pl.Float64, 'PREMREC': pl.Float64}
        )
        print(f"  ✓ Loaded EQFL: {len(eqfl):,} rows")
    except Exception as e:
        print(f"  ✗ Error loading EQFL: {e}")
        return

    try:
        cra_path = get_input_path("CRA", date_vars)
        if not os.path.exists(cra_path):
            cra_path_txt = cra_path + ".txt"
            if not os.path.exists(cra_path_txt):
                print(f"  ✗ CRA file not found at {cra_path}")
                return
            cra_path = cra_path_txt
        
        print(f"  Loading CRA from: {cra_path}")
        cra = load_cra_file(cra_path)
        print(f"  ✓ Loaded CRA: {len(cra):,} rows")
        
        if len(cra) > 0:
            print(f"  CRA sample data (first 2 rows):")
            print(cra.head(2))
            print(f"  Available INV_STATUS values: {cra['INV_STATUS'].unique().to_list()}")
        
        cra = ensure_join_key_type(cra, 'INVCURAC', pl.Int64)
    except Exception as e:
        print(f"  ✗ Error loading CRA: {e}")
        return

    try:
        eqrate_path = get_input_path("EQRATE", date_vars)
        eqrt = load_sas_file_fast(eqrate_path)
        print(f"  ✓ Loaded EQRATE: {len(eqrt):,} rows")
    except Exception as e:
        print(f"  ✗ Error loading EQRATE: {e}")
        return

    try:
        mnitb_saving = load_mnitb_with_cache(
            get_input_path("MNITB_SAVING", date_vars),
            PARQUET_CACHE["MNITB_SAVING"],
            columns_to_keep=['ACCTNO', 'CUSTCODE']
        )
        print(f"  ✓ Loaded MNITB Saving: {len(mnitb_saving):,} rows")
        
        mnitb_current = load_mnitb_with_cache(
            get_input_path("MNITB_CURRENT", date_vars),
            PARQUET_CACHE["MNITB_CURRENT"],
            columns_to_keep=['ACCTNO', 'CUSTCODE']
        )
        print(f"  ✓ Loaded MNITB Current: {len(mnitb_current):,} rows")
    except Exception as e:
        print(f"  ✗ Error loading MNITB files: {e}")
        return

    try:
        dcid_path = get_input_path("DCID", date_vars)
        dcid = load_sas_file_fast(dcid_path, columns_to_keep=['TICKETNO', 'CUSTCODE'])
        print(f"  ✓ Loaded DCID: {len(dcid):,} rows (only TICKETNO, CUSTCODE)")
    except Exception as e:
        print(f"  ✗ Error loading DCID: {e}")
        return

    print("\n" + "=" * 80)

    # -------------------------------------------------------------------
    # Process DPST
    # -------------------------------------------------------------------
    print("\nProcessing DPST...")
    dpst = dpfl.with_columns([pl.col("ACCINT").cast(pl.Float64)])
    dpst = dpst.join(dcid, on="TICKETNO", how="left")
    print(f"  DPST after merge: {len(dpst):,} rows")

    # -------------------------------------------------------------------
    # Process EQ data
    # -------------------------------------------------------------------
    print("\nProcessing EQ data...")
    eq = eqfl.with_columns([
        pl.col("ACCINTRM").abs(),
        pl.col("ACCINTAMT").abs(),
        pl.col("TOTINTAMT").abs(),
        pl.col("PREMPAID").abs(),
        pl.col("PREMREC").abs(),
    ])
    eq = eq.filter((pl.col("STARTDT") <= str(today)) & (pl.col("MATDT") >= str(today)))
    print(f"  EQ after date filter: {len(eq):,} rows")

    eqc = eq.filter(pl.col("TYPE") == "C")
    eqi = eq.filter(pl.col("TYPE") != "C")
    eqc = eqc.select(EQC_KEEP_COLS)
    eqi = eqi.select(EQI_KEEP_COLS)
    print(f"  EQC: {len(eqc):,} rows, EQI: {len(eqi):,} rows")

    # -------------------------------------------------------------------
    # Customer Leg
    # -------------------------------------------------------------------
    print("\nProcessing Customer Leg...")
    
    # Process CRA data
    dp_cra = cra.filter(pl.col("INV_STATUS").is_in(REPORT_CONFIG["VALID_STATUSES"]))
    print(f"  CRA after status filter: {len(dp_cra):,} rows")
    
    if len(dp_cra) == 0:
        print("  Warning: No CRA records with valid status")
        if len(cra) > 0:
            print(f"  Available INV_STATUS values: {cra['INV_STATUS'].unique().to_list()}")
        dp_cra = pl.DataFrame()
    else:
        dp_cra = dp_cra.with_columns([
            pl.lit("Outstanding").alias("STATUSIND"),
            pl.lit(REPORT_CONFIG["MYR_CURRENCY"]).alias("INVCURR"),
            pl.lit(0.0).alias("PREMPAID"),
            pl.lit(0.0).alias("ACCINT")
        ])

    # Create DEPO dataset
    depo = pl.concat([mnitb_saving, mnitb_current])
    depo = depo.rename({"ACCTNO": "INVCURAC"})
    depo = ensure_join_key_type(depo, 'INVCURAC', pl.Int64)

    # Join CRA with DEPO
    if len(dp_cra) > 0 and len(depo) > 0:
        dp_cra = dp_cra.join(depo, on="INVCURAC", how="inner")
        dp_cra = dp_cra.filter(pl.col("CUSTCODE") >= REPORT_CONFIG["MIN_CUSTCODE"])
        print(f"  CRA after DEPO join: {len(dp_cra):,} rows")

    # Process DCI customer data
    eqdci = dpst.join(eqc, on="TICKETNO", how="inner")
    eqdci = eqdci.filter(pl.col("CUSTCODE") >= REPORT_CONFIG["MIN_CUSTCODE"])
    print(f"  EQDCI after join: {len(eqdci):,} rows")

    # Combine CRA and EQDCI
    if len(dp_cra) > 0 and len(eqdci) > 0:
        eqdci = pl.concat([dp_cra, eqdci])
        print(f"  Combined CRA + EQDCI: {len(eqdci):,} rows")
    elif len(dp_cra) > 0:
        eqdci = dp_cra
        print(f"  Using only CRA data: {len(eqdci):,} rows")
    elif len(eqdci) > 0:
        print(f"  Using only EQDCI data: {len(eqdci):,} rows")
    else:
        eqdci = pl.DataFrame()
        print("  Warning: No customer data available!")

    # FX enrichment
    eqrt = eqrt.rename({"CURRENCY": "INVCURR", "SPOTRATE": "SPOTRT"})
    
    # Set SPOTRT = 1.0 for MYR
    eqrt = eqrt.with_columns([
        pl.when(pl.col("INVCURR") == "MYR")
        .then(1.0000000)
        .otherwise(pl.col("SPOTRT"))
        .alias("SPOTRT")
    ])
    
    if len(eqdci) > 0:
        eqdci = eqdci.join(eqrt.select(['INVCURR', 'SPOTRT']), on="INVCURR", how="left")
        eqdci = eqdci.with_columns([
            pl.col("SPOTRT").fill_null(1.0000000)
        ])

        eqdci = eqdci.with_columns([
            (pl.col("ACCINT") * pl.col("SPOTRT")).alias("ACCINTRM"),
            (pl.col("PREMPAID") * pl.col("SPOTRT")).alias("PREMPAIDRM")
        ])

        cusmyr = eqdci.filter(pl.col("INVCURR") == REPORT_CONFIG["MYR_CURRENCY"])
        cusfcy = eqdci.filter(pl.col("INVCURR") != REPORT_CONFIG["MYR_CURRENCY"])
        print(f"  Customer MYR: {len(cusmyr):,} rows, FCY: {len(cusfcy):,} rows")
    else:
        cusmyr = pl.DataFrame()
        cusfcy = pl.DataFrame()

    # -------------------------------------------------------------------
    # Interbank leg
    # -------------------------------------------------------------------
    print("\nProcessing Interbank Leg...")
    ibnmyr = pl.DataFrame()
    ibnfcy = pl.DataFrame()
    
    if len(eqi) > 0:
        eqdci_ib = eqi.filter(pl.col("FISSCODE") >= "80")
        eqdci_ib = eqdci_ib.join(eqrt.select(['INVCURR', 'SPOTRT']), on="INVCURR", how="left")
        eqdci_ib = eqdci_ib.with_columns([
            pl.col("SPOTRT").fill_null(1.0000000),
            (pl.col("PREMREC") * pl.col("SPOTRT")).alias("PREMRECRM")
        ])
        ibnmyr = eqdci_ib.filter(pl.col("INVCURR") == REPORT_CONFIG["MYR_CURRENCY"])
        ibnfcy = eqdci_ib.filter(pl.col("INVCURR") != REPORT_CONFIG["MYR_CURRENCY"])
        print(f"  Interbank MYR: {len(ibnmyr):,} rows, FCY: {len(ibnfcy):,} rows")

    # -------------------------------------------------------------------
    # Write DCITXT
    # -------------------------------------------------------------------
    text_path = get_output_path("TEXT", date_vars)
    ensure_directory(text_path)
    
    print(f"\nWriting DCITXT output to {text_path}...")
    timestamp = datetime.now().strftime("%H:%M %A, %B %d, %Y")
    
    with open(text_path, "w") as f:
        # Customer MYR section
        f.write(" " * 52 + "PUBLIC BANK BERHAD" + " " * 60 + f"{timestamp}   1\n")
        f.write(f"{' ' * 82}DAILY EXTRACTION OF DCI/CRA CUSTOMER FOR MYR AS AT {RDATE}\n")
        f.write(" Obs CUSTICKETNO                    TICKETNO CUSTNAME                    CUSTCODE BRANCH    INVCURAC    ALTCURAC INVCURR ALTCURR     INVAMT     ALTAMT TENOR      SPOTRT    DCIRT STATUSIND        STARTDT    MATDT     ACCINT   ACCINTRM   PREMPAID PREMPAIDRM\n")
        
        obs = 1
        for row in cusmyr.iter_rows(named=True):
            startdt = format_date_for_display(row.get('STARTDT', ''))
            matdt = format_date_for_display(row.get('MATDT', ''))
            
            accint = scale_value(row.get('ACCINT', 0))
            accintrm = scale_value(row.get('ACCINTRM', 0))
            prempaid = scale_value(row.get('PREMPAID', 0))
            prempaidrm = scale_value(row.get('PREMPAIDRM', 0))
            
            tenor = row.get('TENOR', 0)
            try:
                tenor = int(float(tenor)) if tenor else 0
            except:
                tenor = 0
            
            row_str = (f"{obs:>4} "
                      f"{str(row.get('CUSTICKETNO', '')):>26} "
                      f"{str(row.get('TICKETNO', '')):>10} "
                      f"{str(row.get('CUSTNAME', '')):>30} "
                      f"{row.get('CUSTCODE', 0):>8} "
                      f"{str(row.get('BRANCH', '')):>8} "
                      f"{str(row.get('INVCURAC', '')):>12} "
                      f"{str(row.get('ALTCURAC', '')):>12} "
                      f"{str(row.get('INVCURR', '')):>8} "
                      f"{str(row.get('ALTCURR', '')):>8} "
                      f"{float(row.get('INVAMT', 0)):>12,.2f} "
                      f"{float(row.get('ALTAMT', 0)):>12,.2f} "
                      f"{tenor:>6} "
                      f"{float(row.get('SPOTRT', 0)):>12.7f} "
                      f"{float(row.get('DCIRT', 0)):>8.5f} "
                      f"{str(row.get('STATUSIND', '')):>12} "
                      f"{startdt:>12} "
                      f"{matdt:>12} "
                      f"{accint:>10,.2f} "
                      f"{accintrm:>10,.2f} "
                      f"{prempaid:>10,.2f} "
                      f"{prempaidrm:>10,.2f}")
            f.write(row_str + "\n")
            obs += 1
        
        # Summary line
        if len(cusmyr) > 0:
            total_accint = scale_value(cusmyr['ACCINT'].sum())
            total_accintrm = scale_value(cusmyr['ACCINTRM'].sum())
            total_prempaid = scale_value(cusmyr['PREMPAID'].sum())
            total_prempaidrm = scale_value(cusmyr['PREMPAIDRM'].sum())
            
            f.write(f"{' ' * 77}{'=' * 10} {'=' * 10} {'=' * 10} {'=' * 10}\n")
            f.write(f"{' ' * 77}{total_accint:>10,.2f} {total_accintrm:>10,.2f} {total_prempaid:>10,.2f} {total_prempaidrm:>10,.2f}\n")

    print(f"  ✓ DCITXT written to {text_path}")

    # -------------------------------------------------------------------
    # Build DCI
    # -------------------------------------------------------------------
    print("\nBuilding DCI final output...")
    
    # Combine customer and interbank data - use safe concat
    dcimyr = safe_concat([cusmyr, ibnmyr])
    
    if len(dcimyr) > 0:
        print(f"  Combined data for DCI: {len(dcimyr)} rows")
        
        # Determine which premium column exists
        if 'PREMPAID' in dcimyr.columns and 'PREMREC' in dcimyr.columns:
            # Both exist - use TYPE to decide
            dcimyr = dcimyr.with_columns([
                pl.when(pl.col("TYPE") == "C")
                .then(pl.col("PREMPAID"))
                .otherwise(pl.col("PREMREC"))
                .alias("PREMIUM"),
                pl.lit(today).alias("REPTDATS")
            ])
        elif 'PREMPAID' in dcimyr.columns:
            # Only PREMPAID exists (customer data only)
            dcimyr = dcimyr.with_columns([
                pl.col("PREMPAID").alias("PREMIUM"),
                pl.lit(today).alias("REPTDATS")
            ])
        elif 'PREMREC' in dcimyr.columns:
            # Only PREMREC exists (interbank data only)
            dcimyr = dcimyr.with_columns([
                pl.col("PREMREC").alias("PREMIUM"),
                pl.lit(today).alias("REPTDATS")
            ])
        else:
            # No premium column found
            dcimyr = dcimyr.with_columns([
                pl.lit(0.0).alias("PREMIUM"),
                pl.lit(today).alias("REPTDATS")
            ])
            print("  Warning: No PREMPAID or PREMREC column found, using 0 for PREMIUM")

        def calc_elday(d):
            dd = d.day
            mm = d.month
            yy = d.year
            elday = ELDAY_MAPPING.get(dd, 'DAYX')
            if mm in (4, 6, 9, 11) and dd == 30:
                elday = 'DAYI'
            if mm == 2:
                if dd == 28:
                    elday = 'DAYI'
                    if yy % 4 == 0:
                        elday = 'DAYF'
                if dd == 29 and yy % 4 == 0:
                    elday = 'DAYI'
            return elday

        dcimyr = dcimyr.with_columns([
            pl.col("REPTDATS").map_elements(calc_elday).alias("ELDAY")
        ])

        records = []
        for row in dcimyr.iter_rows(named=True):
            accintrm = row.get("ACCINTRM", 0)
            if accintrm not in (None, 0):
                # Scale ACCINTRM if needed
                amount = scale_value(accintrm)
                records.append({
                    "BNMCODE": BNM_CODES["ACCINTRM"],
                    "ELDAY": row["ELDAY"],
                    "REPTDATS": row["REPTDATS"],
                    "AMOUNT": amount
                })
            
            premium = row.get("PREMIUM", 0)
            if premium not in (None, 0):
                # Scale PREMIUM if needed
                amount = scale_value(premium)
                records.append({
                    "BNMCODE": BNM_CODES["PREMIUM"],
                    "ELDAY": row["ELDAY"],
                    "REPTDATS": row["REPTDATS"],
                    "AMOUNT": amount
                })

        dci_final = pl.DataFrame(records)

        if len(dci_final) > 0:
            dci_final = dci_final.group_by(["BNMCODE", "ELDAY", "REPTDATS"]).agg(
                pl.sum("AMOUNT").alias("AMOUNT")
            )
            print(f"  DCI final: {len(dci_final):,} aggregated records")
        else:
            dci_final = pl.DataFrame()
            print("  No DCI records generated")
    else:
        dci_final = pl.DataFrame()
        print("  No data available to build DCI")

    # -------------------------------------------------------------------
    # Write outputs
    # -------------------------------------------------------------------
    print("\nWriting output files...")
    
    parquet_path = get_output_path("PARQUET", date_vars)
    if len(dci_final) > 0:
        dci_final.write_parquet(parquet_path)
        print(f"  ✓ Parquet written: {parquet_path}")

    sas_path = get_output_path("SAS", date_vars)
    if len(dci_final) > 0:
        print(f"\nWriting SAS dataset to {sas_path}...")
        write_sas_file(dci_final, sas_path)

    csv_path = get_output_path("CSV", date_vars)
    if len(dci_final) > 0:
        dci_final.write_csv(csv_path)
        print(f"  ✓ CSV written: {csv_path}")

    print("\n" + "=" * 80)
    print(f"EIBDCITX completed successfully for {RDATE}!")
    print("=" * 80)

if __name__ == "__main__":
    main()
