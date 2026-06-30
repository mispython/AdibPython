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

# Output Paths
OUTPUT_PATHS = {
    "PARQUET": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/DCI_{date}.parquet",
    "CSV": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/DCI_{date}.csv",
    "SAS": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/BNMK_DCI{mon}{wk}.sas7bdat",
    "TEXT": "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/DCITXT.txt",
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
    "MIN_CUSTCODE": 80,           # Minimum customer code filter
    "VALID_STATUSES": ["ACT", "CEP", "CEU", "CCU", "CMU"],
    "JPY_CURRENCY": "JPY",
    "MYR_CURRENCY": "MYR",
    "DECIMAL_PLACES_JPY": 0,
    "DECIMAL_PLACES_OTHER": 2
}

# ELDAY mapping (day of month to ELDAY code)
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
    """
    Load a fixed-width file by reading as raw text and parsing with slice operations
    """
    try:
        # Try to read with specified encoding
        with open(file_path, 'r', encoding=encoding) as f:
            lines = f.readlines()
    except UnicodeDecodeError:
        # If encoding fails, try Latin-1 (which handles all byte values)
        print(f"  Warning: UTF-8 decoding failed, trying Latin-1 encoding for {file_path}")
        with open(file_path, 'r', encoding='latin-1') as f:
            lines = f.readlines()
    except Exception as e:
        # If all else fails, try binary mode and decode with Latin-1
        print(f"  Warning: Reading as binary with Latin-1 decoding for {file_path}")
        with open(file_path, 'rb') as f:
            content = f.read()
            # Try to decode with Latin-1 (maps all bytes to Unicode)
            text = content.decode('latin-1', errors='replace')
            lines = text.splitlines(keepends=True)
    
    # Parse each line based on fixed widths
    data = []
    for line in lines:
        if line.strip():  # Skip empty lines
            row = {}
            start = 0
            for i, width in enumerate(widths):
                # Extract the field
                field = line[start:start+width].strip()
                col_name = columns[i]
                
                # Convert to appropriate type if specified
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
                    else:  # Utf8
                        row[col_name] = field
                else:
                    row[col_name] = field
                
                start += width
            
            data.append(row)
    
    return pl.DataFrame(data)

def load_sas_file_fast(file_path, columns_to_keep=None):
    """
    Load SAS file using pyreadstat with column filtering for performance
    """
    try:
        # Try to read with column selection if pyreadstat supports it
        try:
            # Attempt to use columns parameter (newer versions)
            df, meta = pyreadstat.read_sas7bdat(file_path, columns=columns_to_keep)
            return pl.DataFrame(df)
        except TypeError:
            # Fallback: read full file then select columns
            print(f"  Note: pyreadstat doesn't support column selection, loading full file then filtering...")
            df, meta = pyreadstat.read_sas7bdat(file_path)
            pl_df = pl.DataFrame(df)
            if columns_to_keep:
                # Select only the columns we need
                existing_cols = [col for col in columns_to_keep if col in pl_df.columns]
                if existing_cols:
                    return pl_df.select(existing_cols)
            return pl_df
    except Exception as e:
        print(f"Error loading SAS file {file_path}: {e}")
        raise

def load_eqfl_file(file_path, separator='|', columns=None, dtypes=None):
    """
    Load EQFL file (pipe-delimited, no header) with proper column names
    """
    # Read the file as text with proper encoding
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except UnicodeDecodeError:
        with open(file_path, 'r', encoding='latin-1') as f:
            lines = f.readlines()
    
    # Parse each line
    data = []
    for line in lines:
        line = line.strip()
        if line:  # Skip empty lines
            # Split by pipe separator
            fields = line.split(separator)
            
            # If we have more fields than columns, trim extra
            if len(fields) > len(columns):
                fields = fields[:len(columns)]
            # If we have fewer fields, pad with None
            elif len(fields) < len(columns):
                fields.extend([None] * (len(columns) - len(fields)))
            
            row = {}
            for i, col_name in enumerate(columns):
                field = fields[i].strip() if i < len(fields) and fields[i] else ''
                
                # Convert to appropriate type if specified
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
                    else:  # Utf8
                        row[col_name] = field
                else:
                    row[col_name] = field
            
            data.append(row)
    
    return pl.DataFrame(data)

# ===================================================================
# Main Processing
# ===================================================================

def main():
    # -------------------------------------------------------------------
    # Step 1: Reporting Date Setup - Use YESTERDAY instead of today
    # -------------------------------------------------------------------
    # Get yesterday's date
    yesterday = date.today() - timedelta(days=1)
    
    # Use yesterday for all date calculations
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

    # Ensure output directories exist
    for output_path in OUTPUT_PATHS.values():
        ensure_directory(output_path)

    # -------------------------------------------------------------------
    # Step 2: Load raw datasets
    # -------------------------------------------------------------------
    print("\nLoading input files...")
    
    try:
        # Load DPFL (fixed width format)
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
        # Load EQFL (pipe delimited, no header)
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
        # Load CRA (fixed width format - binary/flat file)
        cra_path = get_input_path("CRA", date_vars)
        
        # Check if file exists
        if not os.path.exists(cra_path):
            print(f"  ✗ CRA file not found: {cra_path}")
            print("  Trying with .txt extension...")
            cra_path_txt = cra_path + ".txt"
            if os.path.exists(cra_path_txt):
                cra_path = cra_path_txt
                print(f"  ✓ Found CRA file with .txt extension: {cra_path}")
            else:
                print(f"  ✗ CRA file not found with or without .txt extension")
                return
        
        cra = load_fixed_width_file(
            cra_path,
            FILE_FORMATS["CRA"]["widths"],
            FILE_FORMATS["CRA"]["columns"],
            FILE_FORMATS["CRA"]["dtypes"],
            encoding=FILE_FORMATS["CRA"].get("encoding", "latin1")
        )
        print(f"  ✓ Loaded CRA: {len(cra):,} rows from {cra_path}")
    except Exception as e:
        print(f"  ✗ Error loading CRA: {e}")
        return

    try:
        # Load EQRATE (SAS dataset)
        eqrate_path = get_input_path("EQRATE", date_vars)
        eqrt = load_sas_file_fast(eqrate_path)
        print(f"  ✓ Loaded EQRATE: {len(eqrt):,} rows from {eqrate_path}")
    except Exception as e:
        print(f"  ✗ Error loading EQRATE: {e}")
        return

    try:
        # Load MNITB datasets - FAST method with column filtering
        mnitb_saving_path = get_input_path("MNITB_SAVING", date_vars)
        print(f"  Loading MNITB Saving (fast mode)...")
        mnitb_saving = load_sas_file_fast(
            mnitb_saving_path, 
            columns_to_keep=['ACCTNO', 'CUSTCODE']
        )
        print(f"  ✓ Loaded MNITB Saving: {len(mnitb_saving):,} rows (only 2 columns)")
        
        mnitb_current_path = get_input_path("MNITB_CURRENT", date_vars)
        print(f"  Loading MNITB Current (fast mode)...")
        mnitb_current = load_sas_file_fast(
            mnitb_current_path,
            columns_to_keep=['ACCTNO', 'CUSTCODE']
        )
        print(f"  ✓ Loaded MNITB Current: {len(mnitb_current):,} rows (only 2 columns)")
    except Exception as e:
        print(f"  ✗ Error loading MNITB files: {e}")
        return

    try:
        # Load DCID (SAS dataset)
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
    dpst = dpfl.with_columns([
        pl.col("ACCINT").cast(pl.Float64)
    ])

    # Merge with DCID
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

    # Filter by date range using yesterday's date
    eq = eq.filter((pl.col("STARTDT") <= str(today)) & (pl.col("MATDT") >= str(today)))
    print(f"  EQ after date filter: {len(eq):,} rows")

    eqc = eq.filter(pl.col("TYPE") == "C")
    eqi = eq.filter(pl.col("TYPE") != "C")

    # Keep only necessary columns
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

    # CRA processing
    dp_cra = cra.filter(pl.col("INV_STATUS").is_in(REPORT_CONFIG["VALID_STATUSES"]))
    dp_cra = dp_cra.with_columns([
        pl.lit("Outstanding").alias("STATUSIND"),
        pl.lit(REPORT_CONFIG["MYR_CURRENCY"]).alias("INVCURR"),
        pl.lit(0.0).alias("PREMPAID"),
        pl.lit(0.0).alias("ACCINT")
    ])

    # Create DEPO dataset - Now both have same columns (ACCTNO, CUSTCODE)
    depo = pl.concat([mnitb_saving, mnitb_current])
    depo = depo.rename({"ACCTNO": "INVCURAC"})
    print(f"  DEPO combined: {len(depo):,} rows")

    # Join CRA with DEPO
    dp_cra = dp_cra.join(depo, on="INVCURAC", how="inner")
    dp_cra = dp_cra.filter(pl.col("CUSTCODE") >= REPORT_CONFIG["MIN_CUSTCODE"])
    print(f"  CRA after processing: {len(dp_cra):,} rows")

    # Combine EQDCI with CRA
    eqdci = pl.concat([eqdci, dp_cra])
    print(f"  Combined EQDCI: {len(eqdci):,} rows")

    # FX enrichment
    eqrt = eqrt.rename({"CURRENCY": "INVCURR", "SPOTRATE": "SPOTRT"})
    eqdci = eqdci.join(eqrt.select(['INVCURR', 'SPOTRT']), on="INVCURR", how="left")

    # Round based on currency
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

    # Split into MYR and FCY
    cusmyr = eqdci.filter(pl.col("INVCURR") == REPORT_CONFIG["MYR_CURRENCY"])
    cusfcy = eqdci.filter(pl.col("INVCURR") != REPORT_CONFIG["MYR_CURRENCY"])
    print(f"  Customer MYR: {len(cusmyr):,} rows, FCY: {len(cusfcy):,} rows")

    # Write text output for customer
    text_path = get_output_path("TEXT", date_vars)
    ensure_directory(text_path)
    
    print(f"\nWriting customer text output to {text_path}...")
    with open(text_path, "w") as f:
        # Customer MYR
        f.write("PUBLIC BANK BERHAD\n")
        f.write(f"DAILY EXTRACTION OF DCI/CRA CUSTOMER FOR MYR AS AT {RDATE}\n")
        cols = ['CUSTICKETNO', 'TICKETNO', 'CUSTNAME', 'CUSTCODE', 'BRANCH',
                'INVCURAC', 'ALTCURAC', 'INVCURR', 'ALTCURR', 'INVAMT', 'ALTAMT',
                'TENOR', 'SPOTRT', 'DCIRT', 'STATUSIND', 'STARTDT', 'MATDT',
                'ACCINT', 'ACCINTRM', 'PREMPAID', 'PREMPAIDRM']
        f.write(','.join(cols) + '\n')
        for row in cusmyr.iter_rows(named=True):
            f.write(','.join([str(row.get(c, '')) for c in cols]) + '\n')

        # Customer FCY
        f.write("\nPUBLIC BANK BERHAD\n")
        f.write(f"DAILY EXTRACTION OF DCI/CRA CUSTOMER FOR FCY AS AT {RDATE}\n")
        for row in cusfcy.iter_rows(named=True):
            f.write(','.join([str(row.get(c, '')) for c in cols]) + '\n')

    # -------------------------------------------------------------------
    # Step 6: Interbank leg
    # -------------------------------------------------------------------
    print("\nProcessing Interbank Leg...")
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

    # Split interbank
    ibnmyr = eqdci_ib.filter(pl.col("INVCURR") == REPORT_CONFIG["MYR_CURRENCY"])
    ibnfcy = eqdci_ib.filter(pl.col("INVCURR") != REPORT_CONFIG["MYR_CURRENCY"])
    print(f"  Interbank MYR: {len(ibnmyr):,} rows, FCY: {len(ibnfcy):,} rows")

    # Write text output for interbank
    with open(text_path, "a") as f:
        # Interbank MYR
        f.write("\nPUBLIC BANK BERHAD\n")
        f.write(f"DAILY EXTRACTION OF DCI INTERBANK FOR MYR AS AT {RDATE}\n")
        cols = ['CUSTICKETNO', 'TICKETNO', 'CUSTNAME', 'CUSTRES', 'CUSTLOC',
                'FISSCODE', 'EQCUSTYP', 'BRANCH', 'INVCURR', 'ALTCURR',
                'INVAMT', 'ALTAMT', 'TENOR', 'SPOTRT', 'STATUSIND',
                'STARTDT', 'MATDT', 'PREMREC', 'PREMRECRM']
        f.write(','.join(cols) + '\n')
        for row in ibnmyr.iter_rows(named=True):
            f.write(','.join([str(row.get(c, '')) for c in cols]) + '\n')

        # Interbank FCY
        f.write("\nPUBLIC BANK BERHAD\n")
        f.write(f"DAILY EXTRACTION OF DCI INTERBANK FOR FCY AS AT {RDATE}\n")
        for row in ibnfcy.iter_rows(named=True):
            f.write(','.join([str(row.get(c, '')) for c in cols]) + '\n')

    print(f"  ✓ Text output written to {text_path}")

    # -------------------------------------------------------------------
    # Step 7: Build DCI
    # -------------------------------------------------------------------
    print("\nBuilding DCI final output...")
    dcimyr = pl.concat([cusmyr, ibnmyr])

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
        
        # Base mapping
        elday = ELDAY_MAPPING.get(dd, 'DAYX')
        
        # Month adjustments
        if mm in (4, 6, 9, 11) and dd == 30:
            elday = 'DAYI'
        
        # February adjustments
        if mm == 2:
            if dd == 28:
                elday = 'DAYI'
                if yy % 4 == 0:  # Leap year
                    elday = 'DAYF'
            if dd == 29 and yy % 4 == 0:
                elday = 'DAYI'
        
        return elday

    dcimyr = dcimyr.with_columns([
        pl.col("REPT
