# eibdcitx.py - COMPLETE PRODUCTION VERSION
import polars as pl
from datetime import date, datetime, timedelta
import pyreadstat
import os
import codecs
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

EQC_KEEP_COLS = ['TICKETNO','CUSTICKETNO','BRANCH','INVCURR','ALTCURR',
                 'INVAMT','ALTAMT','TENOR','STATUSIND','DCIRT','STARTDT',
                 'MATDT','PREMPAID','TYPE']

EQI_KEEP_COLS = ['TICKETNO','CUSTNAME','CUSTRES','CUSTLOC','FISSCODE',
                 'CUSTICKETNO','BRANCH','INVCURR','ALTCURR','EQCUSTYP',
                 'INVAMT','ALTAMT','TENOR','STATUSIND','STARTDT',
                 'MATDT','PREMREC','TYPE']

BNM_CODES = {
    "ACCINTRM": "4911095000000Y",
    "PREMIUM": "4929996000000Y"
}

REPORT_CONFIG = {
    "MIN_CUSTCODE": 80,
    "VALID_STATUSES": ["ACT","CEP","CEU","CCU","CMU"],
    "JPY_CURRENCY": "JPY",
    "MYR_CURRENCY": "MYR",
    "DECIMAL_PLACES_JPY": 0,
    "DECIMAL_PLACES_OTHER": 2
}

ELDAY_MAPPING = {
    1:'DAYA', 9:'DAYA', 16:'DAYA', 23:'DAYA',
    2:'DAYB',10:'DAYB', 17:'DAYB', 24:'DAYB',
    3:'DAYC',11:'DAYC', 18:'DAYC', 25:'DAYC',
    4:'DAYD',12:'DAYD', 19:'DAYD', 26:'DAYD',
    5:'DAYE',13:'DAYE', 20:'DAYE', 27:'DAYE',
    6:'DAYF',14:'DAYF', 21:'DAYF', 28:'DAYF',
    7:'DAYG',29:'DAYG',
    30:'DAYH',
    8:'DAYI',15:'DAYI', 22:'DAYI', 31:'DAYI'
}

# ===================================================================
# CRA EBCDIC / PACKED-DECIMAL LAYOUT
# ===================================================================
CRA_LAYOUT = [
    ('BRANCH',              0,   3, 'text',   0),
    ('CUSTICKETNO',         6,  60, 'text',   0),
    ('INVCURAC',           66,   6, 'pd',     0),
    ('CUSTNAME',           72, 140, 'text',   0),
    ('INVAMT',            442,   7, 'pd',     2),
    ('STARTDT',           449,  10, 'date',   0),
    ('MATDT',             459,  10, 'date',   0),
    ('DCIRT',             476,   7, 'pd',     7),
    ('TENOR',             485,   2, 'pd',     0),
    ('INV_STATUS',        487,   3, 'text',   0),
    ('ACCINT',            493,   8, 'pd',     6),
    ('CUSTCODE_DB2',      838,   2, 'zoned',  0),
]

CRA_RECORD_LENGTH = 942

# ===================================================================
# HELPER FUNCTIONS
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

def unpack_packed_decimal(raw_bytes, decimal_places=0):
    if not raw_bytes:
        return None
    digits = []
    for b in raw_bytes[:-1]:
        digits.append((b >> 4) & 0x0F)
        digits.append(b & 0x0F)
    last_byte = raw_bytes[-1]
    digits.append((last_byte >> 4) & 0x0F)
    sign_nibble = last_byte & 0x0F
    if any(d > 9 for d in digits):
        return None
    digit_str = ''.join(str(d) for d in digits)
    value = int(digit_str) if digit_str else 0
    if sign_nibble == 0xD:
        value = -value
    if decimal_places > 0:
        value = value / (10 ** decimal_places)
    return value

def decode_ebcdic_text(raw_bytes):
    try:
        return raw_bytes.decode('cp037').strip()
    except Exception:
        return raw_bytes.decode('cp037', errors='replace').strip()

def parse_ebcdic_yymmdd10(text):
    text = text.strip()
    if not text:
        return None
    text = text.replace('/', '-')
    parts = text.split('-')
    if len(parts) == 3 and all(p.isdigit() for p in parts):
        yyyy, mm, dd = parts
        return f"{yyyy}-{mm.zfill(2)}-{dd.zfill(2)}"
    return text

def load_cra_ebcdic_file(file_path, record_length=CRA_RECORD_LENGTH, layout=CRA_LAYOUT):
    with open(file_path, 'rb') as f:
        raw = f.read()
    total_len = len(raw)
    if total_len == 0:
        schema = {}
        for name, _, _, kind, _ in layout:
            schema[name] = pl.Float64 if kind == 'pd' else (pl.Int64 if kind == 'zoned' else pl.Utf8)
        return pl.DataFrame(schema=schema)
    if total_len % record_length != 0:
        print(f"  Warning: CRA file size ({total_len} bytes) not multiple of {record_length}")
    num_records = total_len // record_length
    data = []
    for i in range(num_records):
        rec = raw[i * record_length: (i + 1) * record_length]
        if not rec.strip(b'\x00'):
            continue
        row = {}
        for field_name, start, length, kind, decimals in layout:
            chunk = rec[start:start + length]
            if kind == 'text':
                row[field_name] = decode_ebcdic_text(chunk)
            elif kind == 'date':
                row[field_name] = parse_ebcdic_yymmdd10(decode_ebcdic_text(chunk))
            elif kind == 'pd':
                row[field_name] = unpack_packed_decimal(chunk, decimals)
            elif kind == 'zoned':
                text = decode_ebcdic_text(chunk)
                try:
                    row[field_name] = int(text) if text.strip() else None
                except ValueError:
                    row[field_name] = None
        data.append(row)
    if not data:
        schema = {}
        for name, _, _, kind, _ in layout:
            schema[name] = pl.Float64 if kind == 'pd' else (pl.Int64 if kind == 'zoned' else pl.Utf8)
        return pl.DataFrame(schema=schema)
    df = pl.DataFrame(data)
    if 'INVCURAC' in df.columns:
        df = df.with_columns(pl.col('INVCURAC').cast(pl.Int64, strict=False))
    if 'TENOR' in df.columns:
        df = df.with_columns(pl.col('TENOR').cast(pl.Int64, strict=False))
    return df

def load_fixed_width_file(file_path, widths, columns, dtypes=None, encoding='utf-8', implied_decimals=None):
    implied_decimals = implied_decimals or {}
    try:
        with open(file_path, 'r', encoding=encoding) as f:
            lines = f.readlines()
    except UnicodeDecodeError:
        with open(file_path, 'r', encoding='latin-1') as f:
            lines = f.readlines()
    except Exception:
        with open(file_path, 'rb') as f:
            content = f.read()
        lines = content.decode('latin-1', errors='replace').splitlines(keepends=True)
    data = []
    for line in lines:
        if not line.strip():
            continue
        row = {}
        start = 0
        for i, width in enumerate(widths):
            field = line[start:start+width].strip()
            col_name = columns[i]
            decimals = implied_decimals.get(col_name)
            if decimals is not None:
                try:
                    if field:
                        sign = -1 if field.startswith('-') else 1
                        digits_str = field.lstrip('-').strip()
                        row[col_name] = sign * int(digits_str) / (10 ** decimals) if digits_str else None
                    else:
                        row[col_name] = None
                except Exception:
                    row[col_name] = None
            elif dtypes and col_name in dtypes:
                dtype = dtypes[col_name]
                if dtype == pl.Int64:
                    try:
                        row[col_name] = int(field) if field else None
                    except Exception:
                        row[col_name] = None
                elif dtype == pl.Float64:
                    try:
                        row[col_name] = float(field) if field else None
                    except Exception:
                        row[col_name] = None
                else:
                    row[col_name] = field
            else:
                row[col_name] = field
            start += width
        data.append(row)
    if data:
        return pl.DataFrame(data)
    schema = {col: (dtypes[col] if dtypes and col in dtypes else pl.Utf8) for col in columns}
    return pl.DataFrame(schema=schema)

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
        if not line:
            continue
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
                    except Exception:
                        row[col_name] = None
                elif dtype == pl.Float64:
                    try:
                        row[col_name] = float(field) if field else None
                    except Exception:
                        row[col_name] = None
                else:
                    row[col_name] = field
            else:
                row[col_name] = field
        data.append(row)
    if data:
        return pl.DataFrame(data)
    schema = {col: (dtypes[col] if dtypes and col in dtypes else pl.Utf8) for col in columns}
    return pl.DataFrame(schema=schema)

def load_sas_file_fast(file_path, columns_to_keep=None):
    try:
        try:
            df, _ = pyreadstat.read_sas7bdat(file_path, columns=columns_to_keep)
            return pl.DataFrame(df)
        except TypeError:
            print(f"  Note: pyreadstat doesn't support column selection, loading full file then filtering...")
            df, _ = pyreadstat.read_sas7bdat(file_path)
            pl_df = pl.DataFrame(df)
            if columns_to_keep:
                existing = [c for c in columns_to_keep if c in pl_df.columns]
                if existing:
                    return pl_df.select(existing)
            return pl_df
    except Exception as e:
        print(f"Error loading SAS file {file_path}: {e}")
        raise

def load_mnitb_with_cache(file_path, cache_path, columns_to_keep=None):
    if columns_to_keep is None:
        columns_to_keep = ['ACCTNO', 'CUSTCODE']
    ensure_directory(cache_path)
    if os.path.exists(cache_path):
        print(f"  ✓ Loading from Parquet cache: {cache_path}")
        return pl.read_parquet(cache_path)
    print(f"  Converting SAS to Parquet (first time, this may take a moment)...")
    try:
        try:
            df, _ = pyreadstat.read_sas7bdat(file_path, columns=columns_to_keep)
            pl_df = pl.DataFrame(df)
        except TypeError:
            print(f"  Note: pyreadstat doesn't support column selection, loading full file then filtering...")
            df, _ = pyreadstat.read_sas7bdat(file_path)
            pl_df = pl.DataFrame(df)
            existing = [c for c in columns_to_keep if c in pl_df.columns]
            if existing:
                pl_df = pl_df.select(existing)
        pl_df.write_parquet(cache_path)
        print(f"  ✓ Converted and cached: {cache_path}")
        return pl_df
    except Exception as e:
        print(f"  ✗ Error loading SAS file {file_path}: {e}")
        raise

def ensure_join_key_type(df, column_name, target_type=pl.Int64):
    if column_name in df.columns:
        return df.with_columns([pl.col(column_name).cast(target_type)])
    return df

def format_ddmmyy8(date_str):
    if not date_str:
        return ''
    s = str(date_str).strip()
    if not s:
        return ''
    if len(s) == 8 and s[2] == '/' and s[5] == '/':
        return s
    s = s.replace('/', '-')
    parts = s.split('-')
    if len(parts) == 3 and all(p.strip().isdigit() for p in parts):
        yyyy, mm, dd = parts
        yy = yyyy[-2:] if len(yyyy) == 4 else yyyy.zfill(2)
        return f"{dd.zfill(2)}/{mm.zfill(2)}/{yy}"
    return s

def safe_concat(frames):
    real_frames = [f for f in frames if f.width > 0]
    if not real_frames:
        return pl.DataFrame()
    if len(real_frames) == 1:
        return real_frames[0]
    aligned_frames = []
    for f in real_frames:
        if 'CUSTCODE' in f.columns:
            f = f.with_columns([pl.col('CUSTCODE').cast(pl.Int64, strict=False)])
        aligned_frames.append(f)
    return pl.concat(aligned_frames, how="diagonal_relaxed")

def to_float(val):
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None

def to_int(val):
    if val is None:
        return None
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return None

def write_sas_file(df, file_path):
    """
    Write DataFrame to SAS7BDAT using multiple methods.
    Tries pyreadstat (if available), then saspy, then CSV fallback.
    """
    # Method 1: Try pyreadstat (version 1.1.0+ supports writing)
    try:
        import pyreadstat
        # Check if write_sas7bdat exists in pyreadstat
        if hasattr(pyreadstat, 'write_sas7bdat'):
            print("  Writing SAS dataset using pyreadstat...")
            df_pd = df.to_pandas()
            pyreadstat.write_sas7bdat(df_pd, file_path)
            if os.path.exists(file_path):
                print(f"  ✓ SAS dataset written using pyreadstat: {file_path}")
                return True
    except (ImportError, AttributeError, Exception) as e:
        print(f"  Note: pyreadstat write not available")
    
    # Method 2: Try saspy with proper libref handling
    try:
        import saspy
        print("  Connecting to SAS session...")
        sas = saspy.SASsession()
        
        df_pd = df.to_pandas()
        filename = os.path.basename(file_path)
        dataset_name = filename.replace('.sas7bdat', '')
        output_dir = os.path.dirname(file_path)
        
        # Ensure output directory exists
        ensure_directory(file_path)
        
        # Write to WORK library first (this always works)
        print(f"  Writing dataset to WORK library: {dataset_name}...")
        sas.df2sd(df_pd, table=dataset_name, libref='work')
        
        # Now create the permanent library and copy from WORK
        print(f"  Creating permanent library at: {output_dir}")
        sas.submit(f'''
            libname outlib "{output_dir}";
            proc copy in=work out=outlib;
                select {dataset_name};
            run;
        ''')
        
        # Verify the file was created
        if os.path.exists(file_path):
            print(f"  ✓ SAS dataset written: {file_path}")
            return True
        else:
            print(f"  Warning: SAS dataset may not have been written to {file_path}")
            return False
            
    except ImportError:
        print("  ⚠ saspy not available.")
    except Exception as e:
        print(f"  ✗ saspy error: {e}")
    
    # Method 3: Fallback to CSV
    csv_path = file_path.replace('.sas7bdat', '.csv')
    df.write_csv(csv_path)
    print(f"  ✓ CSV fallback written: {csv_path}")
    print(f"  To convert to SAS, use:")
    print(f"    proc import datafile='{csv_path}' out={os.path.basename(file_path).replace('.sas7bdat', '')} dbms=csv replace;")
    return False

# ===================================================================
# DCITXT FORMATTING - EXACTLY MATCHING SAS PROC PRINT
# ===================================================================

def write_cus_row(f_obj, obs, row):
    """Write a customer row with safe type conversion"""
    # Get values with safe conversion
    custcode = to_int(row.get('CUSTCODE', 0)) or 0
    invamt = to_float(row.get('INVAMT', 0)) or 0.0
    altamt = to_float(row.get('ALTAMT', 0)) or 0.0
    tenor = to_int(row.get('TENOR', 0)) or 0
    spotrt = to_float(row.get('SPOTRT', 0)) or 0.0
    dcirt = to_float(row.get('DCIRT', 0)) or 0.0
    accint = to_float(row.get('ACCINT', 0)) or 0.0
    accintrm = to_float(row.get('ACCINTRM', 0)) or 0.0
    prempaid = to_float(row.get('PREMPAID', 0)) or 0.0
    prempaidrm = to_float(row.get('PREMPAIDRM', 0)) or 0.0
    
    f_obj.write(
        f"{obs:>4} "
        f"{str(row.get('CUSTICKETNO','') or ''):<26} "
        f"{str(row.get('TICKETNO','') or ''):<10} "
        f"{str(row.get('CUSTNAME','') or ''):<30} "
        f"{custcode:>8} "
        f"{str(row.get('BRANCH','') or ''):<8} "
        f"{str(row.get('INVCURAC','') or ''):<12} "
        f"{str(row.get('ALTCURAC','') or ''):<12} "
        f"{str(row.get('INVCURR','') or ''):<8} "
        f"{str(row.get('ALTCURR','') or ''):<8} "
        f"{invamt:>12.2f} "
        f"{altamt:>12.2f} "
        f"{tenor:>6} "
        f"{spotrt:>12.7f} "
        f"{dcirt:>8.5f} "
        f"{str(row.get('STATUSIND','') or ''):<12} "
        f"{format_ddmmyy8(row.get('STARTDT','')):>12} "
        f"{format_ddmmyy8(row.get('MATDT','')):>12} "
        f"{accint:>10.2f} "
        f"{accintrm:>10.2f} "
        f"{prempaid:>10.2f} "
        f"{prempaidrm:>10.2f}\n"
    )

def write_ibn_row(f_obj, obs, row):
    """Write an interbank row with safe type conversion"""
    invamt = to_float(row.get('INVAMT', 0)) or 0.0
    altamt = to_float(row.get('ALTAMT', 0)) or 0.0
    tenor = to_int(row.get('TENOR', 0)) or 0
    spotrt = to_float(row.get('SPOTRT', 0)) or 0.0
    premrec = to_float(row.get('PREMREC', 0)) or 0.0
    premreccrm = to_float(row.get('PREMRECRM', 0)) or 0.0
    
    f_obj.write(
        f"{obs:>4} "
        f"{str(row.get('CUSTICKETNO','') or ''):<20} "
        f"{str(row.get('TICKETNO','') or ''):<10} "
        f"{str(row.get('CUSTNAME','') or ''):<30} "
        f"{str(row.get('CUSTRES','') or ''):<8} "
        f"{str(row.get('CUSTLOC','') or ''):<10} "
        f"{str(row.get('FISSCODE','') or ''):<10} "
        f"{str(row.get('EQCUSTYP','') or ''):<10} "
        f"{str(row.get('BRANCH','') or ''):<8} "
        f"{str(row.get('INVCURR','') or ''):<8} "
        f"{str(row.get('ALTCURR','') or ''):<8} "
        f"{invamt:>12.2f} "
        f"{altamt:>12.2f} "
        f"{tenor:>6} "
        f"{spotrt:>12.7f} "
        f"{str(row.get('STATUSIND','') or ''):<12} "
        f"{format_ddmmyy8(row.get('STARTDT','')):>12} "
        f"{format_ddmmyy8(row.get('MATDT','')):>12} "
        f"{premrec:>10.2f} "
        f"{premreccrm:>10.2f}\n"
    )

# ===================================================================
# MAIN PROCESSING
# ===================================================================

def main():
    today = date.today() - timedelta(days=1)
    REPTDAY = f"{today.day:02d}"
    REPTMON = f"{today.month:02d}"
    REPTYEAR = f"{today.year % 100:02d}"
    REPTYEAR4 = f"{today.year:04d}"
    RDATE = today.strftime("%d/%m/%y")

    day = today.day
    if 1 <= day <= 8: WK = "1"
    elif 9 <= day <= 15: WK = "2"
    elif 16 <= day <= 22: WK = "3"
    else: WK = "4"

    date_vars = {
        'yyyy': REPTYEAR4, 'yy': REPTYEAR, 'mm': REPTMON, 'dd': REPTDAY,
        'date': f"{REPTYEAR}{REPTMON}{REPTDAY}",
        'day': REPTDAY, 'mon': REPTMON, 'year': REPTYEAR,
        'wk': WK, 'rdate': RDATE
    }

    print(f"Running EIBDCITX for {RDATE} (WK={WK})")
    print("=" * 80)

    for output_path in OUTPUT_PATHS.values():
        ensure_directory(output_path)

    # -------------------------------------------------------------------
    # Load datasets
    # -------------------------------------------------------------------
    print("\nLoading input files...")

    try:
        dpfl_path = get_input_path("DPFL", date_vars)
        dpfl = load_fixed_width_file(
            dpfl_path,
            [7, 26, 20, 5, 11, 11, 15],
            ['TICKETNO', 'CUSTNAME', 'NEWIC', 'CUSTCODE', 'INVCURAC', 'ALTCURAC', 'ACCINT'],
            {'TICKETNO': pl.Utf8, 'CUSTNAME': pl.Utf8, 'NEWIC': pl.Utf8,
             'CUSTCODE': pl.Int64, 'INVCURAC': pl.Int64, 'ALTCURAC': pl.Int64, 'ACCINT': pl.Float64},
            implied_decimals={'ACCINT': 6}
        )
        print(f"  ✓ Loaded DPFL: {len(dpfl):,} rows")
    except Exception as e:
        print(f"  ✗ Error loading DPFL: {e}"); return

    try:
        eqfl_path = get_input_path("EQFL", date_vars)
        eqfl = load_eqfl_file(
            eqfl_path,
            separator='|',
            columns=[
                'CUSTICKETNO','TICKETNO','BRANCH','CUSTNAME','DEALID',
                'FISSCODE','CUSTRES','CUSTMNE','CUSTLOC','EQCUSTYP',
                'PRODUCT','INVCURR','ALTCURR','INVAMT','INVAMTRM',
                'ALTAMT','TRADEDT','STARTDT','FIXINGDT','MATDT',
                'STOPDT','TENOR','STRIKERT','SPOTRT','DCIRT',
                'ACCINTAMT','TOTINTAMT','ACCINTRM','MMRT','RSPOTRT',
                'PREMREC','PREMPAID','PROFIT','PROFITMYR','UNWINDCOST',
                'STATUSIND','STATUS','TYPE'
            ],
            dtypes={'STARTDT': pl.Utf8, 'MATDT': pl.Utf8, 'ACCINTRM': pl.Float64,
                    'ACCINTAMT': pl.Float64, 'TOTINTAMT': pl.Float64,
                    'PREMPAID': pl.Float64, 'PREMREC': pl.Float64}
        )
        print(f"  ✓ Loaded EQFL: {len(eqfl):,} rows")
    except Exception as e:
        print(f"  ✗ Error loading EQFL: {e}"); return

    try:
        cra_path = get_input_path("CRA", date_vars)
        if not os.path.exists(cra_path):
            cra_path_txt = cra_path + ".txt"
            if not os.path.exists(cra_path_txt):
                print(f"  ✗ CRA file not found"); return
            cra_path = cra_path_txt
        cra = load_cra_ebcdic_file(cra_path)
        print(f"  ✓ Loaded CRA: {len(cra):,} rows")
        cra = ensure_join_key_type(cra, 'INVCURAC', pl.Int64)
    except Exception as e:
        print(f"  ✗ Error loading CRA: {e}"); return

    try:
        eqrate_path = get_input_path("EQRATE", date_vars)
        eqrt = load_sas_file_fast(eqrate_path)
        print(f"  ✓ Loaded EQRATE: {len(eqrt):,} rows")
    except Exception as e:
        print(f"  ✗ Error loading EQRATE: {e}"); return

    try:
        print(f"  Loading MNITB Saving...")
        mnitb_saving = load_mnitb_with_cache(
            get_input_path("MNITB_SAVING", date_vars),
            PARQUET_CACHE["MNITB_SAVING"],
            columns_to_keep=['ACCTNO', 'CUSTCODE']
        )
        print(f"  ✓ Loaded MNITB Saving: {len(mnitb_saving):,} rows")
        
        print(f"  Loading MNITB Current...")
        mnitb_current = load_mnitb_with_cache(
            get_input_path("MNITB_CURRENT", date_vars),
            PARQUET_CACHE["MNITB_CURRENT"],
            columns_to_keep=['ACCTNO', 'CUSTCODE']
        )
        print(f"  ✓ Loaded MNITB Current: {len(mnitb_current):,} rows")
    except Exception as e:
        print(f"  ✗ Error loading MNITB files: {e}"); return

    try:
        dcid_path = get_input_path("DCID", date_vars)
        dcid = load_sas_file_fast(dcid_path, columns_to_keep=['TICKETNO', 'CUSTCODE'])
        print(f"  ✓ Loaded DCID: {len(dcid):,} rows")
    except Exception as e:
        print(f"  ✗ Error loading DCID: {e}"); return

    print("\n" + "=" * 80)

    # -------------------------------------------------------------------
    # Process DPST
    # -------------------------------------------------------------------
    print("\nProcessing DPST...")
    dpst = dpfl.with_columns([pl.col("ACCINT").cast(pl.Float64)])
    dpst = dpst.join(dcid, on="TICKETNO", how="left")
    if 'CUSTCODE_right' in dpst.columns:
        dpst = dpst.with_columns([
            pl.coalesce([pl.col("CUSTCODE_right"), pl.col("CUSTCODE")]).alias("CUSTCODE")
        ]).drop("CUSTCODE_right")
    if 'CUSTCODE' in dpst.columns:
        dpst = dpst.with_columns([pl.col("CUSTCODE").cast(pl.Int64, strict=False)])
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
        pl.col("PREMREC").abs()
    ])
    eq = eq.filter((pl.col("STARTDT") <= str(today)) & (pl.col("MATDT") >= str(today)))
    print(f"  EQ after date filter: {len(eq):,} rows")

    eqc = eq.filter(pl.col("TYPE") == "C").select(EQC_KEEP_COLS)
    eqi = eq.filter(pl.col("TYPE") != "C").select(EQI_KEEP_COLS)
    print(f"  EQC: {len(eqc):,} rows, EQI: {len(eqi):,} rows")

    # -------------------------------------------------------------------
    # Customer Leg
    # -------------------------------------------------------------------
    print("\nProcessing Customer Leg...")

    eqdci = dpst.join(eqc, on="TICKETNO", how="inner")
    eqdci = eqdci.filter(pl.col("CUSTCODE") >= REPORT_CONFIG["MIN_CUSTCODE"])
    print(f"  EQDCI after join: {len(eqdci):,} rows")

    dp_cra = cra.filter(pl.col("INV_STATUS").is_in(REPORT_CONFIG["VALID_STATUSES"]))
    if len(dp_cra) == 0:
        print("  Note: No CRA records with valid status")
        dp_cra = pl.DataFrame(schema={
            'BRANCH': pl.Utf8, 'CUSTICKETNO': pl.Utf8, 'INVCURAC': pl.Int64,
            'CUSTNAME': pl.Utf8, 'INVAMT': pl.Float64, 'STARTDT': pl.Utf8,
            'MATDT': pl.Utf8, 'DCIRT': pl.Float64, 'TENOR': pl.Int64,
            'INV_STATUS': pl.Utf8, 'ACCINT': pl.Float64, 'CUSTCODE_DB2': pl.Int64,
            'STATUSIND': pl.Utf8, 'INVCURR': pl.Utf8,
            'PREMPAID': pl.Float64, 'TYPE': pl.Utf8
        })
    else:
        dp_cra = dp_cra.with_columns([
            pl.lit("Outstanding").alias("STATUSIND"),
            pl.lit(REPORT_CONFIG["MYR_CURRENCY"]).alias("INVCURR"),
            pl.lit(0.0).alias("PREMPAID"),
            pl.lit(None).cast(pl.Utf8).alias("TYPE")
        ])

    depo = pl.concat([mnitb_saving, mnitb_current])
    depo = depo.rename({"ACCTNO": "INVCURAC"})
    depo = ensure_join_key_type(depo, 'INVCURAC', pl.Int64)
    print(f"  DEPO combined: {len(depo):,} rows")

    if dp_cra.width > 0 and depo.width > 0 and len(dp_cra) > 0:
        dp_cra = dp_cra.join(depo, on="INVCURAC", how="inner")
        if 'CUSTCODE_right' in dp_cra.columns:
            dp_cra = dp_cra.with_columns([
                pl.coalesce([pl.col("CUSTCODE_right"), pl.col("CUSTCODE")]).alias("CUSTCODE")
            ]).drop("CUSTCODE_right")
        if 'CUSTCODE' in dp_cra.columns:
            dp_cra = dp_cra.with_columns([pl.col("CUSTCODE").cast(pl.Int64, strict=False)])
        dp_cra = dp_cra.filter(pl.col("CUSTCODE") >= REPORT_CONFIG["MIN_CUSTCODE"])
        print(f"  CRA after processing: {len(dp_cra):,} rows")
    else:
        print("  Note: No CRA or DEPO data to join")
        dp_cra = pl.DataFrame()

    eqdci = safe_concat([dp_cra, eqdci])
    print(f"  Combined EQDCI: {len(eqdci):,} rows")

    eqdci = eqdci.with_columns([pl.col("ACCINT").round(2)])

    eqrt = eqrt.rename({"CURRENCY": "INVCURR", "SPOTRATE": "SPOTRT"})
    eqdci = eqdci.join(eqrt.select(['INVCURR', 'SPOTRT']), on="INVCURR", how="left")

    eqdci = eqdci.with_columns([
        pl.when(pl.col("INVCURR") == REPORT_CONFIG["JPY_CURRENCY"])
          .then(pl.col("ACCINT").round(REPORT_CONFIG["DECIMAL_PLACES_JPY"]))
          .otherwise(pl.col("ACCINT").round(REPORT_CONFIG["DECIMAL_PLACES_OTHER"]))
          .alias("ACCINTX"),
        pl.when(pl.col("INVCURR") == REPORT_CONFIG["JPY_CURRENCY"])
          .then(pl.col("PREMPAID").round(REPORT_CONFIG["DECIMAL_PLACES_JPY"]))
          .otherwise(pl.col("PREMPAID").round(REPORT_CONFIG["DECIMAL_PLACES_OTHER"]))
          .alias("PREMPAI")
    ])
    eqdci = eqdci.with_columns([
        (pl.col("ACCINTX") * pl.col("SPOTRT")).alias("ACCINTRM"),
        (pl.col("PREMPAI") * pl.col("SPOTRT")).alias("PREMPAIDRM")
    ])

    cusmyr = eqdci.filter(pl.col("INVCURR") == REPORT_CONFIG["MYR_CURRENCY"])
    cusfcy = eqdci.filter(pl.col("INVCURR") != REPORT_CONFIG["MYR_CURRENCY"])
    print(f"  Customer MYR: {len(cusmyr):,} rows, FCY: {len(cusfcy):,} rows")

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
            pl.when(pl.col("INVCURR") == REPORT_CONFIG["JPY_CURRENCY"])
              .then(pl.col("PREMREC").round(REPORT_CONFIG["DECIMAL_PLACES_JPY"]))
              .otherwise(pl.col("PREMREC").round(REPORT_CONFIG["DECIMAL_PLACES_OTHER"]))
              .alias("PREMREX")
        ])
        eqdci_ib = eqdci_ib.with_columns([
            (pl.col("PREMREX") * pl.col("SPOTRT")).alias("PREMRECRM")
        ])
        ibnmyr = eqdci_ib.filter(pl.col("INVCURR") == REPORT_CONFIG["MYR_CURRENCY"])
        ibnfcy = eqdci_ib.filter(pl.col("INVCURR") != REPORT_CONFIG["MYR_CURRENCY"])
        print(f"  Interbank MYR: {len(ibnmyr):,} rows, FCY: {len(ibnfcy):,} rows")
    else:
        print("  No interbank data to process")

    # -------------------------------------------------------------------
    # Write DCITXT - EXACTLY MATCHING SAS PROC PRINT
    # -------------------------------------------------------------------
    text_path = get_output_path("TEXT", date_vars)
    ensure_directory(text_path)
    print(f"\nWriting DCITXT output to {text_path}...")
    timestamp = datetime.now().strftime("%H:%M %A, %B %d, %Y")

    CUS_HDR = (" Obs CUSTICKETNO                    TICKETNO CUSTNAME                    "
               "CUSTCODE BRANCH    INVCURAC    ALTCURAC INVCURR ALTCURR     INVAMT     "
               "ALTAMT TENOR      SPOTRT    DCIRT STATUSIND        STARTDT    MATDT     "
               "ACCINT   ACCINTRM   PREMPAID PREMPAIDRM\n")
    IBN_HDR = (" Obs CUSTICKETNO TICKETNO CUSTNAME                        CUSTRES CUSTLOC"
               "   FISSCODE  EQCUSTYP BRANCH   INVCURR ALTCURR     INVAMT     ALTAMT "
               "TENOR      SPOTRT STATUSIND    STARTDT    MATDT   PREMREC PREMRECRM\n")

    with open(text_path, "w") as f:
        page = 1

        # Customer MYR
        f.write(f"{' '*52}PUBLIC BANK BERHAD{' '*60}{timestamp}   {page}\n")
        f.write(f"{' '*82}DAILY EXTRACTION OF DCI/CRA CUSTOMER FOR MYR AS AT {RDATE}\n")
        f.write(CUS_HDR)
        for obs, row in enumerate(cusmyr.iter_rows(named=True), 1):
            write_cus_row(f, obs, row)
        if len(cusmyr) > 0:
            f.write(f"{' '*77}{'='*10} {'='*10} {'='*10} {'='*10}\n")
            f.write(f"{' '*77}"
                    f"{cusmyr['ACCINT'].sum():>10.2f} "
                    f"{cusmyr['ACCINTRM'].sum():>10.2f} "
                    f"{cusmyr['PREMPAID'].sum():>10.2f} "
                    f"{cusmyr['PREMPAIDRM'].sum():>10.2f}\n")

        # Customer FCY
        if len(cusfcy) > 0:
            page += 1
            ts = datetime.now().strftime("%H:%M %A, %B %d, %Y")
            f.write(f"\n{' '*52}PUBLIC BANK BERHAD{' '*60}{ts}   {page}\n")
            f.write(f"{' '*82}DAILY EXTRACTION OF DCI/CRA CUSTOMER FOR FCY AS AT {RDATE}\n")
            f.write(CUS_HDR)
            for obs, row in enumerate(cusfcy.iter_rows(named=True), 1):
                write_cus_row(f, obs, row)
            if len(cusfcy) > 0:
                f.write(f"{' '*77}{'='*10} {'='*10} {'='*10} {'='*10}\n")
                f.write(f"{' '*77}"
                        f"{cusfcy['ACCINT'].sum():>10.2f} "
                        f"{cusfcy['ACCINTRM'].sum():>10.2f} "
                        f"{cusfcy['PREMPAID'].sum():>10.2f} "
                        f"{cusfcy['PREMPAIDRM'].sum():>10.2f}\n")

        # Interbank MYR
        if len(ibnmyr) > 0:
            page += 1
            ts = datetime.now().strftime("%H:%M %A, %B %d, %Y")
            f.write(f"\n{' '*52}PUBLIC BANK BERHAD{' '*60}{ts}   {page}\n")
            f.write(f"{' '*82}DAILY EXTRACTION OF DCI INTERBANK FOR MYR AS AT {RDATE}\n")
            f.write(IBN_HDR)
            for obs, row in enumerate(ibnmyr.iter_rows(named=True), 1):
                write_ibn_row(f, obs, row)

        # Interbank FCY
        if len(ibnfcy) > 0:
            page += 1
            ts = datetime.now().strftime("%H:%M %A, %B %d, %Y")
            f.write(f"\n{' '*52}PUBLIC BANK BERHAD{' '*60}{ts}   {page}\n")
            f.write(f"{' '*82}DAILY EXTRACTION OF DCI INTERBANK FOR FCY AS AT {RDATE}\n")
            f.write(IBN_HDR)
            for obs, row in enumerate(ibnfcy.iter_rows(named=True), 1):
                write_ibn_row(f, obs, row)

    print(f"  ✓ DCITXT written to {text_path}")

    # -------------------------------------------------------------------
    # Build DCI
    # -------------------------------------------------------------------
    print("\nBuilding DCI final output...")

    dcimyr = pl.DataFrame()
    if cusmyr.width > 0 or ibnmyr.width > 0:
        frames = []
        if cusmyr.width > 0:
            frames.append(cusmyr)
        if ibnmyr.width > 0:
            frames.append(ibnmyr)
        dcimyr = safe_concat(frames)
        print(f"  Combined data: {len(dcimyr)} rows")
    else:
        print("  No data available")

    dci_final = pl.DataFrame()
    if len(dcimyr) > 0:
        dcimyr = dcimyr.with_columns([
            pl.when(pl.col("TYPE") == "C")
              .then(pl.col("PREMPAID") if "PREMPAID" in dcimyr.columns else pl.lit(None).cast(pl.Float64))
              .otherwise(pl.col("PREMREC") if "PREMREC" in dcimyr.columns else pl.lit(None).cast(pl.Float64))
              .alias("PREMIUM"),
            pl.lit(today).alias("REPTDATS")
        ])

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
            pl.col("REPTDATS").map_elements(calc_elday, return_dtype=pl.Utf8).alias("ELDAY")
        ])

        records = []
        for row in dcimyr.iter_rows(named=True):
            accintrm = row.get("ACCINTRM")
            if accintrm not in (None, 0):
                records.append({
                    "BNMCODE": BNM_CODES["ACCINTRM"],
                    "ELDAY": row["ELDAY"],
                    "REPTDATS": row["REPTDATS"],
                    "AMOUNT": accintrm
                })
            premium = row.get("PREMIUM")
            if premium not in (None, 0):
                records.append({
                    "BNMCODE": BNM_CODES["PREMIUM"],
                    "ELDAY": row["ELDAY"],
                    "REPTDATS": row["REPTDATS"],
                    "AMOUNT": premium
                })

        if records:
            dci_final = pl.DataFrame(records).group_by(["BNMCODE", "ELDAY", "REPTDATS"]).agg(
                pl.sum("AMOUNT").alias("AMOUNT")
            )
            print(f"  DCI final: {len(dci_final):,} records")

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
