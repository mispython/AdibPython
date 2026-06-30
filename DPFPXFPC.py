# eibdcitx.py
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from datetime import date, datetime, timedelta
import pyreadstat
import os
from pathlib import Path

# ===================================================================
# PATH CONFIGURATION - Modify these paths as needed
# ===================================================================

# Input Paths
INPUT_PATHS = {
    # Main data files
    "DPFL": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/DPFL.txt",
    "EQFL": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/UTSASDCID_{yyyy}{mm}{dd}.txt",
    "CRA": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/DPCRATXT_{yyyy}{mm}{dd}",
    "EQRATE": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/eqrate{yy}{mm}{dd}.sas7bdat",
    "MNITB_SAVING": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/intg_dp_acct_saving.sas7bdat",
    "MNITB_CURRENT": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/intg_dp_acct_current.sas7bdat",
    "DCID": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/dcid{mm}{dd}.sas7bdat",
}

# Parquet cache paths (for converted files)
PARQUET_CACHE = {
    "MNITB_SAVING": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/cache/intg_dp_acct_saving.parquet",
    "MNITB_CURRENT": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/cache/intg_dp_acct_current.parquet",
}

# Output Paths
OUTPUT_PATHS = {
    "PARQUET": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/DCI_{date}.parquet",
    "CSV": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/DCI_{date}.csv",
    "SAS": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/BNMK_DCI{mon}{wk}.sas7bdat",
    "TEXT": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/DCITXT.txt",
}

# SAS session configuration for saspy
SAS_CONFIG = {
    "sascfg": {
        "saspath": "/usr/local/SASHome/SASFoundation/9.4/bin/sas",
        "options": ["-fullstimer", "-nosyntaxcheck", "-sasuser", "work"],
    }
}

# File format configurations
FILE_FORMATS = {
    "DPFL": {
        "fixed_width": True,
        "widths": [7, 26, 20, 5, 11, 11, 15],
        "columns": ['TICKETNO', 'CUSTNAME', 'NEWIC', 'CUSTCODE',
                   'INVCURAC', 'ALTCURAC', 'ACCINT'],
        "dtypes": {
            'TICKETNO': pl.Utf8,
            'CUSTNAME': pl.Utf8,
            'NEWIC': pl.Utf8,
            'CUSTCODE': pl.Int64,
            'INVCURAC': pl.Int64,
            'ALTCURAC': pl.Int64,
            'ACCINT': pl.Float64
        }
    },
    "CRA": {
        "fixed_width": True,
        "encoding": "latin1",
        "widths": [3, 60, 6, 140, 7, 10, 10, 7, 2, 3, 8, 2],
        "columns": ['BRANCH', 'CUSTICKETNO', 'INVCURAC', 'CUSTNAME',
                   'INVAMT', 'STARTDT', 'MATDT', 'DCIRT', 'TENOR',
                   'INV_STATUS', 'ACCINT', 'CUSTCODE_DB2'],
        "dtypes": {
            'BRANCH': pl.Utf8,
            'CUSTICKETNO': pl.Utf8,
            'INVCURAC': pl.Int64,
            'CUSTNAME': pl.Utf8,
            'INVAMT': pl.Float64,
            'STARTDT': pl.Utf8,
            'MATDT': pl.Utf8,
            'DCIRT': pl.Float64,
            'TENOR': pl.Int64,
            'INV_STATUS': pl.Utf8,
            'ACCINT': pl.Float64,
            'CUSTCODE_DB2': pl.Int64
        }
    },
    "EQFL": {
        "separator": "|",
        "has_header": False,
        "columns": [
            'CUSTICKETNO', 'TICKETNO', 'BRANCH', 'CUSTNAME', 'DEALID',
            'FISSCODE', 'CUSTRES', 'CUSTMNE', 'CUSTLOC', 'EQCUSTYP',
            'PRODUCT', 'INVCURR', 'ALTCURR', 'INVAMT', 'INVAMTRM',
            'ALTAMT', 'TRADEDT', 'STARTDT', 'FIXINGDT', 'MATDT',
            'STOPDT', 'TENOR', 'STRIKERT', 'SPOTRT', 'DCIRT',
            'ACCINTAMT', 'TOTINTAMT', 'ACCINTRM', 'MMRT', 'RSPOTRT',
            'PREMREC', 'PREMPAID', 'PROFIT', 'PROFITMYR', 'UNWINDCOST',
            'STATUSIND', 'STATUS', 'TYPE'
        ],
        "dtypes": {
            'CUSTICKETNO': pl.Utf8,
            'TICKETNO': pl.Utf8,
            'BRANCH': pl.Utf8,
            'CUSTNAME': pl.Utf8,
            'DEALID': pl.Utf8,
            'FISSCODE': pl.Utf8,
            'CUSTRES': pl.Utf8,
            'CUSTMNE': pl.Utf8,
            'CUSTLOC': pl.Utf8,
            'EQCUSTYP': pl.Utf8,
            'PRODUCT': pl.Utf8,
            'INVCURR': pl.Utf8,
            'ALTCURR': pl.Utf8,
            'INVAMT': pl.Float64,
            'INVAMTRM': pl.Float64,
            'ALTAMT': pl.Float64,
            'TRADEDT': pl.Utf8,
            'STARTDT': pl.Utf8,
            'FIXINGDT': pl.Utf8,
            'MATDT': pl.Utf8,
            'STOPDT': pl.Utf8,
            'TENOR': pl.Int64,
            'STRIKERT': pl.Float64,
            'SPOTRT': pl.Float64,
            'DCIRT': pl.Float64,
            'ACCINTAMT': pl.Float64,
            'TOTINTAMT': pl.Float64,
            'ACCINTRM': pl.Float64,
            'MMRT': pl.Float64,
            'RSPOTRT': pl.Float64,
            'PREMREC': pl.Float64,
            'PREMPAID': pl.Float64,
            'PROFIT': pl.Float64,
            'PROFITMYR': pl.Float64,
            'UNWINDCOST': pl.Float64,
            'STATUSIND': pl.Utf8,
            'STATUS': pl.Utf8,
            'TYPE': pl.Utf8
        }
    }
}

# EQC keep columns (matching SAS EQCVAR)
EQC_KEEP_COLS = ['TICKETNO', 'CUSTICKETNO', 'BRANCH', 'INVCURR', 'ALTCURR',
                 'INVAMT', 'ALTAMT', 'TENOR', 'STATUSIND', 'DCIRT', 'STARTDT',
                 'MATDT', 'PREMPAID', 'TYPE']

# EQI keep columns (matching SAS EQIVAR)
EQI_KEEP_COLS = ['TICKETNO', 'CUSTNAME', 'CUSTRES', 'CUSTLOC', 'FISSCODE',
                 'CUSTICKETNO', 'BRANCH', 'INVCURR', 'ALTCURR', 'EQCUSTYP',
                 'INVAMT', 'ALTAMT', 'TENOR', 'STATUSIND', 'STARTDT',
                 'MATDT', 'PREMREC', 'TYPE']

# BNM Codes mapping
BNM_CODES = {
    "ACCINTRM": "4911095000000Y",
    "PREMIUM": "4929996000000Y"
}

# Reporting configuration
REPORT_CONFIG = {
    "MIN_CUSTCODE": 80,
    "VALID_STATUSES": ["ACT", "CEP", "CEU", "CCU", "CMU"],
    "JPY_CURRENCY": "JPY",
    "MYR_CURRENCY": "MYR",
    "DECIMAL_PLACES_JPY": 0,
    "DECIMAL_PLACES_OTHER": 2
}

# ELDAY mapping
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
    """Ensure directory exists"""
    Path(path).parent.mkdir(parents=True, exist_ok=True)

def format_path_with_date(path, date_vars):
    """Replace date placeholders in path with actual values"""
    result = path
    for key, value in date_vars.items():
        result = result.replace(f'{{{key}}}', str(value))
    return result

def get_input_path(file_key, date_vars):
    """Get input file path with date substitutions"""
    path = INPUT_PATHS[file_key]
    return format_path_with_date(path, date_vars)

def get_output_path(file_key, date_vars):
    """Get output file path with date substitutions"""
    path = OUTPUT_PATHS[file_key]
    return format_path_with_date(path, date_vars)

def load_fixed_width_file(file_path, widths, columns, dtypes=None, encoding='utf-8'):
    """Load a fixed-width file by reading as raw text and parsing with slice operations"""
    try:
        with open(file_path, 'r', encoding=encoding) as f:
            lines = f.readlines()
    except UnicodeDecodeError:
        print(f"  Warning: UTF-8 decoding failed, trying Latin-1 encoding for {file_path}")
        with open(file_path, 'r', encoding='latin-1') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"  Warning: Reading as binary with Latin-1 decoding for {file_path}")
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

    if data:
        return pl.DataFrame(data)
    else:
        # Preserve schema even when no rows were read
        schema = {col: (dtypes[col] if dtypes and col in dtypes else pl.Utf8) for col in columns}
        return pl.DataFrame(schema=schema)

def load_sas_file_fast(file_path, columns_to_keep=None):
    """Load SAS file using pyreadstat with column filtering for performance"""
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
    """Load MNITB file with Parquet caching for faster subsequent loads"""
    ensure_directory(cache_path)

    if os.path.exists(cache_path):
        print(f"  ✓ Loading from Parquet cache: {cache_path}")
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
            print(f"  ✗ Error loading SAS file {file_path}: {e}")
            raise

def load_eqfl_file(file_path, separator='|', columns=None, dtypes=None):
    """Load EQFL file (pipe-delimited, no header) with proper column names"""
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

    if data:
        return pl.DataFrame(data)
    else:
        schema = {col: (dtypes[col] if dtypes and col in dtypes else pl.Utf8) for col in columns}
        return pl.DataFrame(schema=schema)

def ensure_join_key_type(df, column_name, target_type=pl.Int64):
    """Ensure a column has the correct type for joining"""
    if column_name in df.columns:
        return df.with_columns([
            pl.col(column_name).cast(target_type)
        ])
    return df

def write_sas_file(df, file_path):
    """Write DataFrame to SAS file using saspy"""
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

        print(f"  ✓ SAS dataset written using saspy: {file_path}")
        return True

    except ImportError:
        print("  ⚠ saspy not installed. Installing saspy...")
        try:
            import subprocess
            subprocess.check_call(['pip', 'install', 'saspy'])
            print("  ✓ saspy installed successfully!")
            return write_sas_file(df, file_path)
        except Exception as e:
            print(f"  ✗ Could not install saspy: {e}")
            csv_path = file_path.replace('.sas7bdat', '.csv')
            df.write_csv(csv_path)
            print(f"  ✓ CSV fallback written: {csv_path}")
            return False
    except Exception as e:
        print(f"  ✗ Could not write SAS file with saspy: {e}")
        csv_path = file_path.replace('.sas7bdat', '.csv')
        df.write_csv(csv_path)
        print(f"  ✓ CSV fallback written: {csv_path}")
        return False

def safe_concat(frames):
    """
    Concatenate a list of Polars DataFrames safely.

    Unlike a naive pl.concat(), this:
      - Skips frames that have zero columns (no schema at all) since
        those cannot be meaningfully combined with anything.
      - Keeps frames that have zero ROWS but a valid schema, because
        an empty-but-typed DataFrame is legitimate and must not be
        silently dropped or substituted with a columnless pl.DataFrame().
      - Uses 'vertical_relaxed' so minor dtype mismatches between
        frames (e.g. Int64 vs Float64 from different empty-filter
        paths) don't raise a SchemaError.
    """
    real_frames = [f for f in frames if f.width > 0]
    if not real_frames:
        return pl.DataFrame()
    if len(real_frames) == 1:
        return real_frames[0]
    return pl.concat(real_frames, how="vertical_relaxed")

# ===================================================================
# Main Processing
# ===================================================================

def main():
    # -------------------------------------------------------------------
    # Step 1: Reporting Date Setup
    # -------------------------------------------------------------------
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

    print(f"Running EIBDCITX for {RDATE} (WK={WK}) - Processing YESTERDAY'S data")
    print("=" * 80)

    for output_path in OUTPUT_PATHS.values():
        ensure_directory(output_path)

    # -------------------------------------------------------------------
    # Step 2: Load raw datasets
    # -------------------------------------------------------------------
    print("\nLoading input files...")

    try:
        dpfl_path = get_input_path("DPFL", date_vars)
        dpfl = load_fixed_width_file(
            dpfl_path,
            FILE_FORMATS["DPFL"]["widths"],
            FILE_FORMATS["DPFL"]["columns"],
            FILE_FORMATS["DPFL"]["dtypes"]
        )
        print(f"  ✓ Loaded DPFL: {len(dpfl):,} rows from {dpfl_path}")
    except Exception as e:
        print(f"  ✗ Error loading DPFL: {e}")
        return

    try:
        eqfl_path = get_input_path("EQFL", date_vars)
        eqfl = load_eqfl_file(
            eqfl_path,
            separator=FILE_FORMATS["EQFL"]["separator"],
            columns=FILE_FORMATS["EQFL"]["columns"],
            dtypes=FILE_FORMATS["EQFL"]["dtypes"]
        )
        print(f"  ✓ Loaded EQFL: {len(eqfl):,} rows from {eqfl_path}")
    except Exception as e:
        print(f"  ✗ Error loading EQFL: {e}")
        return

    try:
        cra_path = get_input_path("CRA", date_vars)
        if not os.path.exists(cra_path):
            cra_path_txt = cra_path + ".txt"
            if os.path.exists(cra_path_txt):
                cra_path = cra_path_txt
            else:
                print(f"  ✗ CRA file not found")
                return

        cra = load_fixed_width_file(
            cra_path,
            FILE_FORMATS["CRA"]["widths"],
            FILE_FORMATS["CRA"]["columns"],
            FILE_FORMATS["CRA"]["dtypes"],
            encoding=FILE_FORMATS["CRA"].get("encoding", "latin1")
        )
        print(f"  ✓ Loaded CRA: {len(cra):,} rows from {cra_path}")
        cra = ensure_join_key_type(cra, 'INVCURAC', pl.Int64)
    except Exception as e:
        print(f"  ✗ Error loading CRA: {e}")
        return

    try:
        eqrate_path = get_input_path("EQRATE", date_vars)
        eqrt = load_sas_file_fast(eqrate_path)
        print(f"  ✓ Loaded EQRATE: {len(eqrt):,} rows from {eqrate_path}")
    except Exception as e:
        print(f"  ✗ Error loading EQRATE: {e}")
        return

    try:
        mnitb_saving_path = get_input_path("MNITB_SAVING", date_vars)
        mnitb_saving_cache = PARQUET_CACHE["MNITB_SAVING"]
        print(f"  Loading MNITB Saving...")
        mnitb_saving = load_mnitb_with_cache(
            mnitb_saving_path,
            mnitb_saving_cache,
            columns_to_keep=['ACCTNO', 'CUSTCODE']
        )
        print(f"  ✓ Loaded MNITB Saving: {len(mnitb_saving):,} rows")

        mnitb_current_path = get_input_path("MNITB_CURRENT", date_vars)
        mnitb_current_cache = PARQUET_CACHE["MNITB_CURRENT"]
        print(f"  Loading MNITB Current...")
        mnitb_current = load_mnitb_with_cache(
            mnitb_current_path,
            mnitb_current_cache,
            columns_to_keep=['ACCTNO', 'CUSTCODE']
        )
        print(f"  ✓ Loaded MNITB Current: {len(mnitb_current):,} rows")
    except Exception as e:
        print(f"  ✗ Error loading MNITB files: {e}")
        return

    try:
        dcid_path = get_input_path("DCID", date_vars)
        dcid = load_sas_file_fast(dcid_path)
        print(f"  ✓ Loaded DCID: {len(dcid):,} rows from {dcid_path}")
    except Exception as e:
        print(f"  ✗ Error loading DCID: {e}")
        return

    print("\n" + "=" * 80)

    # -------------------------------------------------------------------
    # Step 3: DPST dataset
    # -------------------------------------------------------------------
    print("\nProcessing DPST...")
    dpst = dpfl.with_columns([pl.col("ACCINT").cast(pl.Float64)])
    dpst = dpst.join(dcid, on="TICKETNO", how="left")
    print(f"  DPST after merge: {len(dpst):,} rows")

    # -------------------------------------------------------------------
    # Step 4: EQC / EQI split
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

    eqc = eq.filter(pl.col("TYPE") == "C")
    eqi = eq.filter(pl.col("TYPE") != "C")
    eqc = eqc.select(EQC_KEEP_COLS)
    eqi = eqi.select(EQI_KEEP_COLS)
    print(f"  EQC: {len(eqc):,} rows, EQI: {len(eqi):,} rows")

    # -------------------------------------------------------------------
    # Step 5: Customer leg (EQC join DPST, CRA, DEPO)
    # -------------------------------------------------------------------
    print("\nProcessing Customer Leg...")
    eqdci = dpst.join(eqc, on="TICKETNO", how="inner")
    eqdci = eqdci.filter(pl.col("CUSTCODE") >= REPORT_CONFIG["MIN_CUSTCODE"])
    print(f"  EQDCI after join: {len(eqdci):,} rows")

    dp_cra = cra.filter(pl.col("INV_STATUS").is_in(REPORT_CONFIG["VALID_STATUSES"]))
    if len(dp_cra) == 0:
        print("  Note: No CRA records with valid status")
        # Preserve full schema (including columns added below) even when empty
        dp_cra = pl.DataFrame(schema={
            'BRANCH': pl.Utf8, 'CUSTICKETNO': pl.Utf8, 'INVCURAC': pl.Int64,
            'CUSTNAME': pl.Utf8, 'INVAMT': pl.Float64, 'STARTDT': pl.Utf8,
            'MATDT': pl.Utf8, 'DCIRT': pl.Float64, 'TENOR': pl.Int64,
            'INV_STATUS': pl.Utf8, 'ACCINT': pl.Float64, 'CUSTCODE_DB2': pl.Int64
        })

    # Always add derived columns, whether dp_cra has rows or not
    dp_cra = dp_cra.with_columns([
        pl.lit("Outstanding").alias("STATUSIND"),
        pl.lit(REPORT_CONFIG["MYR_CURRENCY"]).alias("INVCURR"),
        pl.lit(0.0).alias("PREMPAID"),
        pl.lit(0.0).alias("ACCINT")
    ]) if dp_cra.width > 0 else dp_cra

    depo = pl.concat([mnitb_saving, mnitb_current])
    depo = depo.rename({"ACCTNO": "INVCURAC"})
    print(f"  DEPO combined: {len(depo):,} rows")
    depo = ensure_join_key_type(depo, 'INVCURAC', pl.Int64)

    if dp_cra.width > 0 and depo.width > 0:
        dp_cra = dp_cra.join(depo, on="INVCURAC", how="inner")
        dp_cra = dp_cra.filter(pl.col("CUSTCODE") >= REPORT_CONFIG["MIN_CUSTCODE"])
        print(f"  CRA after processing: {len(dp_cra):,} rows")
    else:
        print("  Note: No CRA or DEPO data to join")
        dp_cra = pl.DataFrame()

    eqdci = safe_concat([eqdci, dp_cra])
    print(f"  Combined EQDCI: {len(eqdci):,} rows")

    # FX enrichment
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
    # Step 6: Interbank leg
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
    # Step 7: Write DCITXT with exact SAS format
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

        # Write data rows
        obs = 1
        for row in cusmyr.iter_rows(named=True):
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
                      f"{row.get('INVAMT', 0):>12,.2f} "
                      f"{row.get('ALTAMT', 0):>12,.2f} "
                      f"{row.get('TENOR', 0):>6} "
                      f"{row.get('SPOTRT', 0):>12.7f} "
                      f"{row.get('DCIRT', 0):>8.5f} "
                      f"{str(row.get('STATUSIND', '')):>12} "
                      f"{row.get('STARTDT', ''):>12} "
                      f"{row.get('MATDT', ''):>12} "
                      f"{row.get('ACCINT', 0):>10,.2f} "
                      f"{row.get('ACCINTRM', 0):>10,.2f} "
                      f"{row.get('PREMPAID', 0):>10,.2f} "
                      f"{row.get('PREMPAIDRM', 0):>10,.2f}")
            f.write(row_str + "\n")
            obs += 1

        # Summary line for MYR
        if len(cusmyr) > 0:
            total_accint = cusmyr['ACCINT'].sum()
            total_accintrm = cusmyr['ACCINTRM'].sum()
            total_prempaid = cusmyr['PREMPAID'].sum()
            total_prempaidrm = cusmyr['PREMPAIDRM'].sum()

            f.write(f"{' ' * 77}{'=' * 10} {'=' * 10} {'=' * 10} {'=' * 10}\n")
            f.write(f"{' ' * 77}{total_accint:>10,.2f} {total_accintrm:>10,.2f} {total_prempaid:>10,.2f} {total_prempaidrm:>10,.2f}\n")

        # Customer FCY section (if any)
        if len(cusfcy) > 0:
            timestamp2 = datetime.now().strftime("%H:%M %A, %B %d, %Y")
            f.write(f"\n{' ' * 52}PUBLIC BANK BERHAD{' ' * 60}{timestamp2}   2\n")
            f.write(f"{' ' * 82}DAILY EXTRACTION OF DCI/CRA CUSTOMER FOR FCY AS AT {RDATE}\n")
            f.write(" Obs CUSTICKETNO                    TICKETNO CUSTNAME                    CUSTCODE BRANCH    INVCURAC    ALTCURAC INVCURR ALTCURR     INVAMT     ALTAMT TENOR      SPOTRT    DCIRT STATUSIND        STARTDT    MATDT     ACCINT   ACCINTRM   PREMPAID PREMPAIDRM\n")

            obs = 1
            for row in cusfcy.iter_rows(named=True):
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
                          f"{row.get('INVAMT', 0):>12,.2f} "
                          f"{row.get('ALTAMT', 0):>12,.2f} "
                          f"{row.get('TENOR', 0):>6} "
                          f"{row.get('SPOTRT', 0):>12.7f} "
                          f"{row.get('DCIRT', 0):>8.5f} "
                          f"{str(row.get('STATUSIND', '')):>12} "
                          f"{row.get('STARTDT', ''):>12} "
                          f"{row.get('MATDT', ''):>12} "
                          f"{row.get('ACCINT', 0):>10,.2f} "
                          f"{row.get('ACCINTRM', 0):>10,.2f} "
                          f"{row.get('PREMPAID', 0):>10,.2f} "
                          f"{row.get('PREMPAIDRM', 0):>10,.2f}")
                f.write(row_str + "\n")
                obs += 1

            # Summary for FCY
            total_accint = cusfcy['ACCINT'].sum()
            total_accintrm = cusfcy['ACCINTRM'].sum()
            total_prempaid = cusfcy['PREMPAID'].sum()
            total_prempaidrm = cusfcy['PREMPAIDRM'].sum()

            f.write(f"{' ' * 77}{'=' * 10} {'=' * 10} {'=' * 10} {'=' * 10}\n")
            f.write(f"{' ' * 77}{total_accint:>10,.2f} {total_accintrm:>10,.2f} {total_prempaid:>10,.2f} {total_prempaidrm:>10,.2f}\n")

        # Interbank MYR section (if any)
        if len(ibnmyr) > 0:
            # Get next page number
            page_num = 3
            timestamp3 = datetime.now().strftime("%H:%M %A, %B %d, %Y")
            f.write(f"\n{' ' * 52}PUBLIC BANK BERHAD{' ' * 60}{timestamp3}   {page_num}\n")
            f.write(f"{' ' * 82}DAILY EXTRACTION OF DCI INTERBANK FOR MYR AS AT {RDATE}\n")
            f.write(" Obs CUSTICKETNO TICKETNO CUSTNAME                        CUSTRES CUSTLOC   FISSCODE  EQCUSTYP BRANCH   INVCURR ALTCURR     INVAMT     ALTAMT TENOR      SPOTRT STATUSIND    STARTDT    MATDT   PREMREC PREMRECRM\n")

            obs = 1
            for row in ibnmyr.iter_rows(named=True):
                row_str = (f"{obs:>4} "
                          f"{str(row.get('CUSTICKETNO', '')):>20} "
                          f"{str(row.get('TICKETNO', '')):>10} "
                          f"{str(row.get('CUSTNAME', '')):>30} "
                          f"{str(row.get('CUSTRES', '')):>8} "
                          f"{str(row.get('CUSTLOC', '')):>10} "
                          f"{str(row.get('FISSCODE', '')):>10} "
                          f"{str(row.get('EQCUSTYP', '')):>10} "
                          f"{str(row.get('BRANCH', '')):>8} "
                          f"{str(row.get('INVCURR', '')):>8} "
                          f"{str(row.get('ALTCURR', '')):>8} "
                          f"{row.get('INVAMT', 0):>12,.2f} "
                          f"{row.get('ALTAMT', 0):>12,.2f} "
                          f"{row.get('TENOR', 0):>6} "
                          f"{row.get('SPOTRT', 0):>12.7f} "
                          f"{str(row.get('STATUSIND', '')):>12} "
                          f"{row.get('STARTDT', ''):>12} "
                          f"{row.get('MATDT', ''):>12} "
                          f"{row.get('PREMREC', 0):>10,.2f} "
                          f"{row.get('PREMRECRM', 0):>10,.2f}")
                f.write(row_str + "\n")
                obs += 1

        # Interbank FCY section (if any)
        if len(ibnfcy) > 0:
            page_num = 4
            timestamp4 = datetime.now().strftime("%H:%M %A, %B %d, %Y")
            f.write(f"\n{' ' * 52}PUBLIC BANK BERHAD{' ' * 60}{timestamp4}   {page_num}\n")
            f.write(f"{' ' * 82}DAILY EXTRACTION OF DCI INTERBANK FOR FCY AS AT {RDATE}\n")
            f.write(" Obs CUSTICKETNO TICKETNO CUSTNAME                        CUSTRES CUSTLOC   FISSCODE  EQCUSTYP BRANCH   INVCURR ALTCURR     INVAMT     ALTAMT TENOR      SPOTRT STATUSIND    STARTDT    MATDT   PREMREC PREMRECRM\n")

            obs = 1
            for row in ibnfcy.iter_rows(named=True):
                row_str = (f"{obs:>4} "
                          f"{str(row.get('CUSTICKETNO', '')):>20} "
                          f"{str(row.get('TICKETNO', '')):>10} "
                          f"{str(row.get('CUSTNAME', '')):>30} "
                          f"{str(row.get('CUSTRES', '')):>8} "
                          f"{str(row.get('CUSTLOC', '')):>10} "
                          f"{str(row.get('FISSCODE', '')):>10} "
                          f"{str(row.get('EQCUSTYP', '')):>10} "
                          f"{str(row.get('BRANCH', '')):>8} "
                          f"{str(row.get('INVCURR', '')):>8} "
                          f"{str(row.get('ALTCURR', '')):>8} "
                          f"{row.get('INVAMT', 0):>12,.2f} "
                          f"{row.get('ALTAMT', 0):>12,.2f} "
                          f"{row.get('TENOR', 0):>6} "
                          f"{row.get('SPOTRT', 0):>12.7f} "
                          f"{str(row.get('STATUSIND', '')):>12} "
                          f"{row.get('STARTDT', ''):>12} "
                          f"{row.get('MATDT', ''):>12} "
                          f"{row.get('PREMREC', 0):>10,.2f} "
                          f"{row.get('PREMRECRM', 0):>10,.2f}")
                f.write(row_str + "\n")
                obs += 1

    print(f"  ✓ DCITXT written to {text_path}")

    # -------------------------------------------------------------------
    # Step 8: Build DCI
    # -------------------------------------------------------------------
    print("\nBuilding DCI final output...")

    # Combine customer and interbank data for DCI.
    # FIX: previously this branched on len(df) > 0 (row count), which
    # caused an empty-but-schema'd DataFrame (e.g. ibnmyr with 0 rows
    # but 57 columns) to be replaced with a columnless pl.DataFrame(),
    # crashing pl.concat with a ShapeError. We now branch on .width
    # (column count) to detect "has a schema" vs "truly absent", and
    # always select() / concat() against the common schema regardless
    # of row count.
    dcimyr = pl.DataFrame()
    if cusmyr.width > 0 or ibnmyr.width > 0:
        cus_cols = set(cusmyr.columns) if cusmyr.width > 0 else set()
        ibn_cols = set(ibnmyr.columns) if ibnmyr.width > 0 else set()

        if cus_cols and ibn_cols:
            common_cols = list(cus_cols & ibn_cols)
        elif cus_cols:
            common_cols = list(cus_cols)
        else:
            common_cols = list(ibn_cols)

        frames = []
        if cus_cols:
            frames.append(cusmyr.select(common_cols))
        if ibn_cols:
            frames.append(ibnmyr.select(common_cols))

        dcimyr = safe_concat(frames)
        print(f"  Combined customer and interbank data: {len(dcimyr)} rows")
    else:
        print("  No data available to build DCI")

    # Process DCI
    if len(dcimyr) > 0:
        # Add PREMIUM column
        dcimyr = dcimyr.with_columns([
            pl.when(pl.col("TYPE") == "C")
            .then(pl.col("PREMPAID"))
            .otherwise(pl.col("PREMREC"))
            .alias("PREMIUM"),
            pl.lit(today).alias("REPTDATS")
        ])

        # Derive ELDAY
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

        # Generate BNM records
        records = []
        for row in dcimyr.iter_rows(named=True):
            accintrm = row.get("ACCINTRM", 0)
            if accintrm not in (None, 0):
                records.append({
                    "BNMCODE": BNM_CODES["ACCINTRM"],
                    "ELDAY": row["ELDAY"],
                    "REPTDATS": row["REPTDATS"],
                    "AMOUNT": accintrm
                })

            premium = row.get("PREMIUM", 0)
            if premium not in (None, 0):
                records.append({
                    "BNMCODE": BNM_CODES["PREMIUM"],
                    "ELDAY": row["ELDAY"],
                    "REPTDATS": row["REPTDATS"],
                    "AMOUNT": premium
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
    # Step 9: Write outputs
    # -------------------------------------------------------------------
    print("\nWriting output files...")

    # Write Parquet
    parquet_path = get_output_path("PARQUET", date_vars)
    ensure_directory(parquet_path)
    if len(dci_final) > 0:
        dci_final.write_parquet(parquet_path)
        print(f"  ✓ Parquet written: {parquet_path}")
    else:
        print(f"  ⚠ No data to write to Parquet: {parquet_path}")

    # Write SAS
    sas_path = get_output_path("SAS", date_vars)
    ensure_directory(sas_path)
    if len(dci_final) > 0:
        print(f"\nWriting SAS dataset to {sas_path}...")
        write_sas_file(dci_final, sas_path)
    else:
        print(f"  ⚠ No data to write to SAS: {sas_path}")

    # Write CSV
    csv_path = get_output_path("CSV", date_vars)
    ensure_directory(csv_path)
    if len(dci_final) > 0:
        dci_final.write_csv(csv_path)
        print(f"  ✓ CSV written: {csv_path}")
    else:
        print(f"  ⚠ No data to write to CSV: {csv_path}")

    print("\n" + "=" * 80)
    print(f"EIBDCITX completed successfully for {RDATE} (Yesterday's data)!")
    print("=" * 80)

if __name__ == "__main__":
    main()
