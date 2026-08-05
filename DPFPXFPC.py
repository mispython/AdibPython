#!/usr/bin/env python3
"""
Program Name: EIBMCOSR
Purpose: Generate Transaction Volume & Cost Analysis Report for Public Bank Berhad (PBB)
         - Processes cost rates, stock banking, deposit collections, ESMR, hardware/datacomm costs
         - Outputs CSV text files (COSTXT01, COSTXT02, COSTEXP) and formatted print report
         - Identifies missing account names and unknown transaction type exceptions
"""

import pyreadstat
import polars as pl
from datetime import date, datetime, timedelta
import os
import glob

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

# Base input directory
INPUT_DIR = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMCOSR"

# Input SAS7BDAT paths (COST library)
COST_NAME_SAS7BDAT      = os.path.join(INPUT_DIR, "name.sas7bdat")
COST_ESMR_SAS7BDAT      = os.path.join(INPUT_DIR, "esmr.sas7bdat")
COST_OTHCOST_SAS7BDAT   = os.path.join(INPUT_DIR, "othcost.sas7bdat")

# Monthly data SAS7BDAT prefix patterns
COST_RATE_PREFIX       = os.path.join(INPUT_DIR, "rate")
COST_STBK_PREFIX       = os.path.join(INPUT_DIR, "stbk")
COST_DPCOL_PREFIX      = os.path.join(INPUT_DIR, "dpcol")
COST_DPECP_PREFIX      = os.path.join(INPUT_DIR, "dpecp")
COST_DPDDS_PREFIX      = os.path.join(INPUT_DIR, "dpdds")
COST_DPMISC_PREFIX     = os.path.join(INPUT_DIR, "dpmisc")
COST_DPBAL_PREFIX      = os.path.join(INPUT_DIR, "dpbal")

# Output paths
OUTPUT_DIR = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output"
COSTXT01_TXT  = os.path.join(OUTPUT_DIR, "cost01_text.txt")
COSTXT02_TXT  = os.path.join(OUTPUT_DIR, "cost02_text.txt")
COSTEXP_TXT   = os.path.join(OUTPUT_DIR, "cost_except_text.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# =============================================================================
# FORMAT / UTILITY HELPERS
# =============================================================================

def sas_date_to_pydate(val):
    """Convert SAS date (datetime.date or other) to Python date."""
    if val is None:
        return None
    if isinstance(val, date):
        return val
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, (int, float)):
        return date(1960, 1, 1) + timedelta(days=int(val))
    if isinstance(val, str):
        return datetime.strptime(val, '%Y-%m-%d').date()
    return val

def parse_ddmmyy8(s):
    """Parse DD/MM/YY string (DDMMYY8. format) to Python date (YEARCUTOFF=1950)."""
    if not s:
        return None
    d, m, y2 = int(s[0:2]), int(s[3:5]), int(s[6:8])
    year = (1900 + y2) if y2 >= 50 else (2000 + y2)
    return date(year, m, d)

def fmt_ddmmyy8(d):
    """Format Python date as DD/MM/YY."""
    if d is None:
        return ''
    return d.strftime('%d/%m/%y')

def fmt_comma(val, width, decimals=2):
    """Format numeric as COMMA<width>.<decimals>, right-justified."""
    if val is None or (isinstance(val, float) and val != val):
        return ' ' * width
    try:
        formatted = f"{float(val):,.{decimals}f}"
    except (TypeError, ValueError):
        return ' ' * width
    return formatted.rjust(width)

def fmt_comma9(val):
    return fmt_comma(val, 9, 0)

def fmt_comma14_2(val):
    return fmt_comma(val, 14, 2)

def fmt_4_2(val):
    """Format as 4.2 (width 4, 2 decimals)."""
    if val is None:
        return '    '
    return f"{float(val):.2f}".rjust(4)

def place_at(line_list, col, text):
    """Place text into line_list at 1-indexed column."""
    col0 = col - 1
    for i, ch in enumerate(str(text)):
        pos = col0 + i
        while len(line_list) <= pos:
            line_list.append(' ')
        line_list[pos] = ch

def make_line(width=132):
    return [' '] * width

def finalize_line(line_list, width=132):
    return ''.join(line_list).ljust(width)[:width]

def coalesce(val, default=0.0):
    if val is None or (isinstance(val, float) and val != val):
        return default
    return val

def normalize_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Convert all column names to lowercase for consistent access."""
    if df.is_empty():
        return df
    col_mapping = {col: col.lower() for col in df.columns}
    return df.rename(col_mapping)

# =============================================================================
# SAS7BDAT READER HELPER
# =============================================================================

def read_sas7bdat(filepath: str) -> pl.DataFrame:
    """Read a SAS7BDAT file using pyreadstat and return a Polars DataFrame with lowercase columns."""
    if not os.path.exists(filepath):
        print(f"  WARNING: File not found: {filepath}")
        return pl.DataFrame()
    try:
        df, meta = pyreadstat.read_sas7bdat(filepath)
        if df.empty:
            print(f"  WARNING: Empty file: {filepath}")
            return pl.DataFrame()
        print(f"  Loaded {os.path.basename(filepath)}: {len(df)} rows")
        result = pl.from_pandas(df)
        # Normalize column names to lowercase
        result = normalize_columns(result)
        return result
    except Exception as e:
        print(f"  ERROR reading {filepath}: {e}")
        return pl.DataFrame()

# =============================================================================
# REPORT DATE VARIABLES (using datetime.timedelta - 1)
# =============================================================================

def get_report_vars():
    """
    Compute all macro variables based on current date minus 1 day.
    Returns dict with: noofday, reptday, reptmon, reptyear, rdate, sdate, smon, emon
    """
    # Get yesterday's date (timedelta - 1)
    reptdate = date.today() - timedelta(days=1)

    # SRPTDATE logic
    srptdate = date(reptdate.year, reptdate.month, 1)

    noofday  = reptdate.day
    reptday  = str(reptdate.day).zfill(2)
    reptmon  = str(reptdate.month).zfill(2)
    reptyear = str(reptdate.year)
    rdate    = fmt_ddmmyy8(reptdate)
    sdate    = fmt_ddmmyy8(srptdate)
    smon     = srptdate.month
    emon     = reptdate.month

    return {
        'noofday':  noofday,
        'reptday':  reptday,
        'reptmon':  reptmon,
        'reptyear': reptyear,
        'rdate':    rdate,
        'sdate':    sdate,
        'smon':     smon,
        'emon':     emon,
        'reptdate': reptdate,
        'srptdate': srptdate,
    }

# =============================================================================
# MONTHLY DATA LOADER
# =============================================================================

def load_monthly_sas7bdat(prefix: str, smon: int, emon: int) -> pl.DataFrame:
    """
    Load and union monthly SAS7BDAT files from smon to emon.
    """
    frames = []
    for i in range(smon, emon + 1):
        month_str = str(i).zfill(2)
        path = f"{prefix}{month_str}.sas7bdat"
        if os.path.exists(path):
            df = read_sas7bdat(path)
            if not df.is_empty():
                frames.append(df)
        else:
            print(f"  File not found: {os.path.basename(path)}")
    
    if not frames:
        print(f"  WARNING: No files found for prefix {os.path.basename(prefix)}, months {smon}-{emon}")
        return pl.DataFrame()
    
    result = pl.concat(frames, how='diagonal')
    print(f"  Combined {len(frames)} files for {os.path.basename(prefix)}: {len(result)} total rows")
    return result

# =============================================================================
# %COSTRT — Load and sort RATE data
# =============================================================================

def load_rate(smon: int, emon: int) -> pl.DataFrame:
    """Load all rate monthly SAS7BDAT files, sort BY trandt."""
    rate = load_monthly_sas7bdat(COST_RATE_PREFIX, smon, emon)
    if rate.is_empty():
        print("  WARNING: No rate data loaded!")
        return rate
    
    # Convert trandt to proper date if it's numeric
    if 'trandt' in rate.columns:
        dtype = rate['trandt'].dtype
        print(f"  TRANDT column dtype: {dtype}")
        if dtype in [pl.Float64, pl.Int64, pl.Float32, pl.Int32]:
            rate = rate.with_columns(
                pl.col('trandt').map_elements(
                    lambda v: sas_date_to_pydate(v) if v is not None else None,
                    return_dtype=pl.Date
                )
            )
    
    return rate.sort('trandt')

# =============================================================================
# %PROCESS — Main transaction processing
# =============================================================================

def process_main(rv: dict, rate: pl.DataFrame) -> tuple:
    """
    Implements %PROCESS macro.
    """
    smon, emon = rv['smon'], rv['emon']
    noofday    = rv['noofday']
    reptyear   = int(rv['reptyear'])

    # Load transaction data
    stbk  = load_monthly_sas7bdat(COST_STBK_PREFIX,  smon, emon)
    dpcol = load_monthly_sas7bdat(COST_DPCOL_PREFIX, smon, emon)
    dpecp = load_monthly_sas7bdat(COST_DPECP_PREFIX, smon, emon)
    dpdds = load_monthly_sas7bdat(COST_DPDDS_PREFIX, smon, emon)
    dpmisc = load_monthly_sas7bdat(COST_DPMISC_PREFIX, smon, emon)

    # DATA TOTSUM: SET STBK DPCOL DPECP DPDDS DPMISC;
    all_frames = [stbk, dpcol, dpecp, dpdds, dpmisc]
    non_empty = [df for df in all_frames if not df.is_empty()]
    
    if not non_empty:
        print("  ERROR: No transaction data loaded!")
        return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()
    
    totsum = pl.concat(non_empty, how='diagonal')
    print(f"  TOTSUM before processing: {len(totsum)} rows")
    
    # Check if trandt exists
    if 'trandt' not in totsum.columns:
        print(f"  ERROR: trandt column not found! Available: {totsum.columns}")
        return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()
    
    # Convert trandt to date if needed
    dtype = totsum['trandt'].dtype
    if dtype in [pl.Float64, pl.Int64, pl.Float32, pl.Int32]:
        totsum = totsum.with_columns(
            pl.col('trandt').map_elements(
                lambda v: sas_date_to_pydate(v) if v is not None else None,
                return_dtype=pl.Date
            )
        )
    
    totsum = totsum.sort('trandt')

    # DATA EXCEPT: rows where svtype not in the known list
    known_svtypes = {'FDSIBG','FDSIBK','ECP','DDS','EBK','TEL','ATM','OTC','ESI',
                     'PREATM','PRESMS','FPXCOL','FPXPYM','STM','PREEBK','MTN',
                     'MIS','PAG','CDT','CDM','MBK'}
    
    if 'svtype' in totsum.columns:
        except_df = totsum.filter(~pl.col('svtype').is_in(list(known_svtypes)))
    else:
        except_df = pl.DataFrame()

    # MERGE TOTSUM + RATE BY trandt (left join)
    if not rate.is_empty() and 'trandt' in rate.columns:
        rate_sel = rate.select([c for c in rate.columns])
        totsum = totsum.join(rate_sel, on='trandt', how='left', suffix='_rate')
        # Resolve rate columns that may overlap
        for col in rate_sel.columns:
            rate_col = f"{col}_rate"
            if rate_col in totsum.columns:
                totsum = totsum.with_columns(
                    pl.when(pl.col(rate_col).is_not_null())
                      .then(pl.col(rate_col))
                      .otherwise(pl.col(col))
                      .alias(col)
                ).drop(rate_col)

    # Compute CNT/COS columns per svtype
    cnt_cos_cols = [
        'cntfds','cosfds','cntecp','cosecp','cntdds','cosdds','cntebk','cosebk',
        'cnttel','costel','cntatm','cosatm','cntotc','cosotc','cntesi','cosesi',
        'cntmisc','cosmisc','cntfpxco','cosfpxco','cntfpxpy','cosfpxpy',
        'cntpag','cospag','cntcdt','coscdt','cntcdm','coscdm','cntmbk','cosmbk'
    ]
    # Initialize all cnt/cos columns
    for c in cnt_cos_cols:
        if c not in totsum.columns:
            totsum = totsum.with_columns(pl.lit(None).cast(pl.Float64).alias(c))

    # Apply row-wise svtype logic
    if 'svtype' in totsum.columns and 'count' in totsum.columns:
        rows_data = totsum.to_dicts()
        cnt_cos_values = {c: [] for c in cnt_cos_cols}
        for row in rows_data:
            svtype   = row.get('svtype', '')
            count    = coalesce(row.get('count', 0), 0.0)
            miscrate = coalesce(row.get('miscrate', 0.0))
            debrate  = coalesce(row.get('debrate',  0.0))
            pberate  = coalesce(row.get('pberate',  0.0))
            telerate = coalesce(row.get('telerate', 0.0))
            atmrate  = coalesce(row.get('atmrate',  0.0))
            otcrate  = coalesce(row.get('otcrate',  0.0))

            out = {c: None for c in cnt_cos_cols}
            if   svtype in ('FDSIBG','FDSIBK'):
                out['cntfds']   = count; out['cosfds']   = count * miscrate
            elif svtype == 'ECP':
                out['cntecp']   = count; out['cosecp']   = count * miscrate
            elif svtype == 'DDS':
                out['cntdds']   = count; out['cosdds']   = count * debrate
            elif svtype == 'EBK':
                out['cntebk']   = count; out['cosebk']   = count * pberate
            elif svtype == 'TEL':
                out['cnttel']   = count; out['costel']   = count * telerate
            elif svtype == 'ATM':
                out['cntatm']   = count; out['cosatm']   = count * atmrate
            elif svtype == 'OTC':
                out['cntotc']   = count; out['cosotc']   = count * otcrate
            elif svtype == 'ESI':
                out['cntesi']   = count; out['cosesi']   = count * miscrate
            elif svtype in ('PREATM','PRESMS','PREEBK','STM','MTN','MIS'):
                out['cntmisc']  = count; out['cosmisc']  = count * miscrate
            elif svtype == 'FPXCOL':
                out['cntfpxco'] = count; out['cosfpxco'] = count * miscrate
            elif svtype == 'FPXPYM':
                out['cntfpxpy'] = count; out['cosfpxpy'] = count * miscrate
            elif svtype == 'PAG':
                out['cntpag']   = count; out['cospag']   = count * miscrate
            elif svtype == 'CDT':
                out['cntcdt']   = count; out['coscdt']   = count * miscrate
            elif svtype == 'CDM':
                out['cntcdm']   = count; out['coscdm']   = count * miscrate
            elif svtype == 'MBK':
                out['cntmbk']   = count; out['cosmbk']   = count * miscrate
            for c in cnt_cos_cols:
                cnt_cos_values[c].append(out[c])

        for c in cnt_cos_cols:
            totsum = totsum.with_columns(pl.Series(c, cnt_cos_values[c], dtype=pl.Float64))

    # TOTFEE = SUM(feeamt1, feeamt2)
    if 'feeamt1' in totsum.columns and 'feeamt2' in totsum.columns:
        totsum = totsum.with_columns(
            (pl.col('feeamt1').fill_null(0.0) + pl.col('feeamt2').fill_null(0.0)).alias('totfee')
        )
    else:
        totsum = totsum.with_columns(pl.lit(0.0).alias('totfee'))

    # Load NAME dataset
    name_df = read_sas7bdat(COST_NAME_SAS7BDAT)
    if not name_df.is_empty():
        name_df = name_df.unique(subset=['acctno'], keep='first').sort('acctno')

    # Sort TOTSUM BY acctno
    if 'acctno' in totsum.columns:
        totsum = totsum.sort('acctno')

    # *** A/C NOT FOUND IN CONTROL FILE ***
    missname = pl.DataFrame()
    if not name_df.is_empty() and 'acctno' in totsum.columns:
        missname = totsum.join(name_df, on='acctno', how='anti')

    # MERGE TOTSUM + NAME BY acctno
    if not name_df.is_empty() and 'acctno' in totsum.columns:
        totsum = totsum.join(name_df, on='acctno', how='left', suffix='_name')
        for col in ['nmabbr', 'custname']:
            name_col = f"{col}_name"
            if name_col in totsum.columns:
                totsum = totsum.with_columns(
                    pl.when(pl.col(name_col).is_not_null())
                      .then(pl.col(name_col))
                      .otherwise(pl.col(col) if col in totsum.columns else pl.lit(None))
                      .alias(col)
                ).drop(name_col)

    # PROC MEANS BY nmabbr SUM
    agg_cols = cnt_cos_cols + ['totfee']
    available_agg = [c for c in agg_cols if c in totsum.columns]
    
    if not totsum.is_empty() and 'nmabbr' in totsum.columns:
        agg_exprs = [pl.col(c).sum().alias(c) for c in available_agg]
        if 'custname' in totsum.columns:
            agg_exprs.append(pl.col('custname').first().alias('custname'))
        totsum = totsum.sort('nmabbr').group_by('nmabbr', maintain_order=True).agg(agg_exprs)

    return totsum, missname, except_df

# =============================================================================
# %ESMRDTL — ESMR detail processing
# =============================================================================

def process_esmr(rv: dict, rate: pl.DataFrame) -> tuple:
    """Implements %ESMRDTL macro."""
    smon    = rv['smon']
    emon    = rv['emon']
    sdate_d = parse_ddmmyy8(rv['sdate'])

    esmr_raw = read_sas7bdat(COST_ESMR_SAS7BDAT)
    if esmr_raw.is_empty():
        print("  WARNING: No ESMR data found")
        return pl.DataFrame(), pl.DataFrame()

    # Convert expdt and trandt from SAS numeric to date
    for col in ['expdt', 'trandt']:
        if col in esmr_raw.columns:
            dtype = esmr_raw[col].dtype
            if dtype in [pl.Float64, pl.Int64, pl.Float32, pl.Int32]:
                esmr_raw = esmr_raw.with_columns(
                    pl.col(col).map_elements(
                        lambda v: sas_date_to_pydate(v) if v is not None else None,
                        return_dtype=pl.Date
                    )
                )

    # IF expdt GE SDATE
    if 'expdt' in esmr_raw.columns:
        esmr_raw = esmr_raw.filter(pl.col('expdt') >= sdate_d)

    # MERGE ESMR + RATE BY trandt
    if not rate.is_empty() and 'trandt' in esmr_raw.columns and 'trandt' in rate.columns:
        esmr_merged = esmr_raw.sort('trandt').join(
            rate.sort('trandt'), on='trandt', how='left', suffix='_rate'
        )
        for col in rate.columns:
            rate_col = f"{col}_rate"
            if rate_col in esmr_merged.columns:
                esmr_merged = esmr_merged.with_columns(
                    pl.when(pl.col(rate_col).is_not_null())
                      .then(pl.col(rate_col))
                      .otherwise(pl.col(col) if col in esmr_merged.columns else pl.lit(None))
                      .alias(col)
                ).drop(rate_col)
    else:
        esmr_merged = esmr_raw

    # COSESMR = (manday * manrate) / 60
    sdate_y, sdate_m = sdate_d.year, sdate_d.month

    if 'expdt' in esmr_merged.columns and 'manday' in esmr_merged.columns and 'manrate' in esmr_merged.columns:
        def compute_mthcount(expdt):
            if expdt is None:
                return None
            return (expdt.year * 12 + expdt.month) - (sdate_y * 12 + sdate_m) + 1

        esmr_merged = esmr_merged.with_columns([
            pl.col('expdt').map_elements(compute_mthcount, return_dtype=pl.Int64).alias('mthcount'),
            ((pl.col('manday') * pl.col('manrate')) / 60.0).alias('cosesmr')
        ])

        frames = []
        for i in range(smon, emon + 1):
            subset = esmr_merged.filter(pl.col('mthcount') >= i)
            frames.append(subset)
        esmr2 = pl.concat(frames, how='diagonal') if frames else pl.DataFrame()
    else:
        esmr2 = esmr_merged

    # NAME lookup
    name_df = read_sas7bdat(COST_NAME_SAS7BDAT)
    if not name_df.is_empty():
        name_df = name_df.unique(subset=['acctno'], keep='first').sort('acctno')

    if name_df.is_empty() or 'acctno' not in esmr2.columns:
        return esmr2, pl.DataFrame()

    esmr2 = esmr2.sort('acctno')
    missacc = esmr2.join(name_df, on='acctno', how='anti')

    esmr2 = esmr2.join(name_df, on='acctno', how='inner', suffix='_name')
    for col in ['nmabbr', 'custname']:
        name_col = f"{col}_name"
        if name_col in esmr2.columns:
            esmr2 = esmr2.with_columns(
                pl.when(pl.col(name_col).is_not_null())
                  .then(pl.col(name_col))
                  .otherwise(pl.col(col) if col in esmr2.columns else pl.lit(None))
                  .alias(col)
            ).drop(name_col)

    if not esmr2.is_empty() and 'nmabbr' in esmr2.columns and 'cosesmr' in esmr2.columns:
        esmr2 = esmr2.sort('nmabbr').group_by('nmabbr', maintain_order=True).agg(
            pl.col('cosesmr').sum()
        )

    return esmr2, missacc

# =============================================================================
# %OTHERDTL — Other cost processing
# =============================================================================

def process_othercost(rv: dict) -> tuple:
    """Implements %OTHERDTL macro."""
    smon    = rv['smon']
    emon    = rv['emon']
    sdate_d = parse_ddmmyy8(rv['sdate'])
    rdate_d = parse_ddmmyy8(rv['rdate'])

    othcost_raw = read_sas7bdat(COST_OTHCOST_SAS7BDAT)
    if othcost_raw.is_empty():
        print("  WARNING: No other cost data found")
        return pl.DataFrame(), pl.DataFrame()

    for dcol in ['expdt', 'trandt']:
        if dcol in othcost_raw.columns:
            dtype = othcost_raw[dcol].dtype
            if dtype in [pl.Float64, pl.Int64, pl.Float32, pl.Int32]:
                othcost_raw = othcost_raw.with_columns(
                    pl.col(dcol).map_elements(
                        lambda v: sas_date_to_pydate(v) if v is not None else None,
                        return_dtype=pl.Date
                    )
                )

    if 'ctype' in othcost_raw.columns and 'expdt' in othcost_raw.columns and 'trandt' in othcost_raw.columns:
        othcost = othcost_raw.filter(
            (pl.col('ctype') == 'DC') |
            ((pl.col('ctype') == 'HW') & (pl.col('expdt') >= sdate_d) & (pl.col('trandt') <= rdate_d))
        )
    else:
        othcost = othcost_raw

    if othcost.is_empty():
        return pl.DataFrame(), pl.DataFrame()

    sdate_y, sdate_m = sdate_d.year, sdate_d.month

    def compute_mthcount(expdt):
        if expdt is None:
            return None
        return (expdt.year * 12 + expdt.month) - (sdate_y * 12 + sdate_m) + 1

    if 'ctype' in othcost.columns and 'oricost' in othcost.columns:
        rows = othcost.to_dicts()
        mthcounts, hwares, datacomms = [], [], []
        for row in rows:
            ctype   = row.get('ctype','')
            oricost = coalesce(row.get('oricost', 0.0))
            expdt   = row.get('expdt')
            if ctype == 'HW':
                mc = compute_mthcount(expdt)
                mthcounts.append(mc)
                hwares.append(oricost / 60.0)
                datacomms.append(None)
            elif ctype == 'DC':
                mthcounts.append(None)
                hwares.append(None)
                datacomms.append(oricost / 12.0)
            else:
                mthcounts.append(None)
                hwares.append(None)
                datacomms.append(None)

        othcost = othcost.with_columns([
            pl.Series('mthcount', mthcounts, dtype=pl.Int64),
            pl.Series('hware',    hwares,    dtype=pl.Float64),
            pl.Series('datacomm', datacomms, dtype=pl.Float64),
        ])

        frames = []
        for i in range(smon, emon + 1):
            subset = othcost.filter((pl.col('mthcount') >= i) | (pl.col('ctype') == 'DC'))
            frames.append(subset)
        othcost2 = pl.concat(frames, how='diagonal') if frames else pl.DataFrame()
    else:
        othcost2 = othcost

    name_df = read_sas7bdat(COST_NAME_SAS7BDAT)
    if not name_df.is_empty():
        name_df = name_df.unique(subset=['acctno'], keep='first').sort('acctno')

    if name_df.is_empty() or 'acctno' not in othcost2.columns:
        return othcost2, pl.DataFrame()

    othcost2 = othcost2.sort('acctno')
    missacc = othcost2.join(name_df, on='acctno', how='anti')

    othcost2 = othcost2.join(name_df, on='acctno', how='inner', suffix='_name')
    for col in ['nmabbr', 'custname']:
        name_col = f"{col}_name"
        if name_col in othcost2.columns:
            othcost2 = othcost2.with_columns(
                pl.when(pl.col(name_col).is_not_null())
                  .then(pl.col(name_col))
                  .otherwise(pl.col(col) if col in othcost2.columns else pl.lit(None))
                  .alias(col)
            ).drop(name_col)

    if not othcost2.is_empty() and 'nmabbr' in othcost2.columns:
        agg_exprs = []
        for c in ['hware', 'datacomm']:
            if c in othcost2.columns:
                agg_exprs.append(pl.col(c).sum())
        if agg_exprs:
            othcost2 = othcost2.sort('nmabbr').group_by('nmabbr', maintain_order=True).agg(agg_exprs)

    return othcost2, missacc

# =============================================================================
# %DEPBAL — Float amount calculation
# =============================================================================

def process_depbal(rv: dict, rate: pl.DataFrame) -> tuple:
    """Implements %DEPBAL macro."""
    smon     = rv['smon']
    emon     = rv['emon']
    noofday  = rv['noofday']
    reptyear = int(rv['reptyear'])

    dpbal = load_monthly_sas7bdat(COST_DPBAL_PREFIX, smon, emon)
    if dpbal.is_empty():
        print("  WARNING: No DPBAL data found")
        return pl.DataFrame(), pl.DataFrame()

    if 'trandt' in dpbal.columns:
        dtype = dpbal['trandt'].dtype
        if dtype in [pl.Float64, pl.Int64, pl.Float32, pl.Int32]:
            dpbal = dpbal.with_columns(
                pl.col('trandt').map_elements(
                    lambda v: sas_date_to_pydate(v) if v is not None else None,
                    return_dtype=pl.Date
                )
            )
        dpbal = dpbal.sort('trandt')

    if not rate.is_empty() and 'trandt' in dpbal.columns and 'trandt' in rate.columns:
        if 'float' in rate.columns:
            rate_float = rate.select(['trandt', 'float'])
        else:
            rate_float = rate.select(['trandt'])
        
        dpbal = dpbal.join(rate_float, on='trandt', how='left', suffix='_rate')
        if 'float_rate' in dpbal.columns:
            dpbal = dpbal.with_columns(
                pl.when(pl.col('float_rate').is_not_null())
                  .then(pl.col('float_rate'))
                  .otherwise(pl.col('float') if 'float' in dpbal.columns else pl.lit(None))
                  .alias('float')
            ).drop('float_rate')

    dayyr = 366 if (reptyear % 4 == 0) else 365

    if 'avgbal' in dpbal.columns:
        def calc_floamt(row):
            avgbal   = coalesce(row.get('avgbal', 0.0))
            float_r  = coalesce(row.get('float',  0.0))
            intrstpd = coalesce(row.get('intrstpd', 0.0))
            if avgbal > 0:
                return ((avgbal * (float_r / 100)) * (noofday / dayyr)) + (-1 * intrstpd)
            return 0.0

        rows   = dpbal.to_dicts()
        floams = [calc_floamt(r) for r in rows]
        dpbal  = dpbal.with_columns(pl.Series('floamt', floams, dtype=pl.Float64))
    else:
        dpbal = dpbal.with_columns(pl.lit(0.0).alias('floamt'))

    name_df = read_sas7bdat(COST_NAME_SAS7BDAT)
    if not name_df.is_empty():
        name_df = name_df.unique(subset=['acctno'], keep='first').sort('acctno')

    if name_df.is_empty() or 'acctno' not in dpbal.columns:
        return dpbal, pl.DataFrame()

    dpbal = dpbal.sort('acctno')
    missacc = dpbal.join(name_df, on='acctno', how='anti')

    dpbal = dpbal.join(name_df, on='acctno', how='inner', suffix='_name')
    for col in ['nmabbr', 'custname']:
        name_col = f"{col}_name"
        if name_col in dpbal.columns:
            dpbal = dpbal.with_columns(
                pl.when(pl.col(name_col).is_not_null())
                  .then(pl.col(name_col))
                  .otherwise(pl.col(col) if col in dpbal.columns else pl.lit(None))
                  .alias(col)
            ).drop(name_col)

    if not dpbal.is_empty() and 'nmabbr' in dpbal.columns and 'floamt' in dpbal.columns:
        dpbal = dpbal.sort('nmabbr').group_by('nmabbr', maintain_order=True).agg(
            pl.col('floamt').sum()
        )

    return dpbal, missacc

# =============================================================================
# GRAND TOTAL MERGE AND FINALIZATION
# =============================================================================

def build_grand_total(totsum: pl.DataFrame, esmr2: pl.DataFrame,
                      othcost2: pl.DataFrame, dpbal: pl.DataFrame,
                      rv: dict, rate: pl.DataFrame) -> pl.DataFrame:
    """Implements the GRAND TOTAL DATA step."""
    if totsum.is_empty():
        print("  WARNING: TOTSUM is empty, cannot build grand total")
        return pl.DataFrame()
        
    rdate_d = parse_ddmmyy8(rv['rdate'])

    name_df = read_sas7bdat(COST_NAME_SAS7BDAT)
    if not name_df.is_empty():
        name_df = name_df.unique(subset=['nmabbr'], keep='first').sort('nmabbr')
        name_df = name_df.select(['custname', 'nmabbr'])

    esmr_join    = esmr2.select(['nmabbr','cosesmr']) if not esmr2.is_empty() and 'cosesmr' in esmr2.columns and 'nmabbr' in esmr2.columns else pl.DataFrame()
    dpbal_join   = dpbal.select(['nmabbr','floamt']) if not dpbal.is_empty() and 'floamt' in dpbal.columns and 'nmabbr' in dpbal.columns else pl.DataFrame()
    othcost_cols = ['nmabbr'] + [c for c in ['hware','datacomm'] if c in othcost2.columns]
    othcost_join = othcost2.select(othcost_cols) if not othcost2.is_empty() and 'nmabbr' in othcost2.columns else pl.DataFrame()

    merged = totsum
    if not esmr_join.is_empty():
        merged = merged.join(esmr_join, on='nmabbr', how='outer', suffix='_esmr')
    if not othcost_join.is_empty():
        merged = merged.join(othcost_join, on='nmabbr', how='left', suffix='_oth')
    if not dpbal_join.is_empty():
        merged = merged.join(dpbal_join, on='nmabbr', how='left', suffix='_bal')
    if not name_df.is_empty():
        merged = merged.join(name_df, on='nmabbr', how='left', suffix='_name')

    if 'cosesmr_esmr' in merged.columns:
        merged = merged.with_columns(
            pl.when(pl.col('cosesmr_esmr').is_not_null())
              .then(pl.col('cosesmr_esmr'))
              .otherwise(pl.col('cosesmr') if 'cosesmr' in merged.columns else pl.lit(None))
              .alias('cosesmr')
        ).drop('cosesmr_esmr')

    if 'custname_name' in merged.columns:
        merged = merged.with_columns(
            pl.when(pl.col('custname_name').is_not_null())
              .then(pl.col('custname_name'))
              .otherwise(pl.col('custname') if 'custname' in merged.columns else pl.lit(None))
              .alias('custname')
        ).drop('custname_name')

    def safe_sum(*cols):
        return sum(coalesce(c) for c in cols)

    rows  = merged.to_dicts()
    todirect_l, tocnt_l, tocost_l, profit_l = [], [], [], []

    for row in rows:
        hware    = coalesce(row.get('hware'))
        datacomm = coalesce(row.get('datacomm'))
        cosesmr  = coalesce(row.get('cosesmr'))
        todirect = safe_sum(hware, datacomm, cosesmr)

        tocnt = safe_sum(
            row.get('cntotc'), row.get('cntatm'), row.get('cntebk'),
            row.get('cnttel'), row.get('cntdds'), row.get('cntecp'),
            row.get('cntfds'), row.get('cntesi'), row.get('cntfpxco'),
            row.get('cosfpxpy'), row.get('cntmisc'),
            row.get('cntpag'), row.get('cntcdt'), row.get('cntcdm'), row.get('cntmbk')
        )
        tocost = safe_sum(
            row.get('cosotc'), row.get('cosatm'), row.get('cosebk'),
            row.get('costel'), row.get('cosdds'), row.get('cosecp'),
            row.get('cosfds'), row.get('cosesi'), row.get('cosfpxco'),
            row.get('cosfpxpy'), row.get('cosmisc'),
            row.get('cospag'), row.get('coscdt'), row.get('coscdm'), row.get('cosmbk')
        )
        totfee  = coalesce(row.get('totfee'))
        floamt  = coalesce(row.get('floamt'))
        profit  = safe_sum(totfee, floamt) + (-1.0 * safe_sum(todirect, tocost))

        todirect_l.append(todirect)
        tocnt_l.append(tocnt)
        tocost_l.append(tocost)
        profit_l.append(profit)

    merged = merged.with_columns([
        pl.Series('todirect', todirect_l, dtype=pl.Float64),
        pl.Series('tocnt',    tocnt_l,    dtype=pl.Float64),
        pl.Series('tocost',   tocost_l,   dtype=pl.Float64),
        pl.Series('profit',   profit_l,   dtype=pl.Float64),
    ])

    merged = merged.with_columns(pl.lit(rdate_d).alias('trandt'))

    if not rate.is_empty() and 'trandt' in rate.columns:
        merged = merged.join(rate, on='trandt', how='left', suffix='_rate')
        for col in rate.columns:
            rate_col = f"{col}_rate"
            if rate_col in merged.columns:
                merged = merged.with_columns(
                    pl.when(pl.col(rate_col).is_not_null())
                      .then(pl.col(rate_col))
                      .otherwise(pl.col(col) if col in merged.columns else pl.lit(None))
                      .alias(col)
                ).drop(rate_col)

    zero_cols = [
        'cntotc','cntatm','cntebk','cnttel','cntdds','cntecp','cntfds','cntesi',
        'cntpag','cntcdt','cntcdm','cntmbk','cntfpxco','cntfpxpy','cntmisc',
        'cosotc','cosatm','cosebk','costel','cosdds','cosecp','cosfds','cosesi',
        'cospag','coscdt','coscdm','cosmbk','cosfpxco','cosfpxpy','cosmisc',
        'totfee','todirect','tocnt','tocost','profit','floamt','hware','datacomm','cosesmr'
    ]
    for c in zero_cols:
        if c in merged.columns:
            merged = merged.with_columns(pl.col(c).fill_null(0.0))

    if 'custname' in merged.columns:
        merged = merged.sort('custname')

    return merged

# =============================================================================
# OUTPUT FUNCTIONS
# =============================================================================

def write_costxt01(totsum: pl.DataFrame, output_path: str):
    """Writes semicolon-delimited file with header and data rows."""
    if totsum.is_empty():
        print("  WARNING: No data to write to COSTXT01")
        with open(output_path, 'w') as f:
            f.write("No data available\n")
        return
        
    with open(output_path, 'w', encoding='ascii', errors='replace') as f:
        f.write(';'
                ';SETUP/DIRECT COST'
                ';' ';' ';'
                ';TOTAL OPERATING'
                ';INCOME(RM)'
                ';' ';'
                ';PROFIT/'
                ';\n')
        f.write('CORPORATE NAME'
                ';TOTAL TXN VOL'
                ';H/W & SYS /SW'
                ';DATA COMM'
                ';PROGRAMMING'
                ';TOTAL COST'
                ';COST(RM)'
                ';TOTAL'
                ';FLOAT'
                ';LOSS(RM)'
                ';\n')
        for row in totsum.iter_rows(named=True):
            custname = str(row.get('custname','') or '')
            tocnt    = str(int(coalesce(row.get('tocnt'), 0)))
            hware    = f"{coalesce(row.get('hware')):15.2f}"
            datacomm = f"{coalesce(row.get('datacomm')):15.2f}"
            cosesmr  = f"{coalesce(row.get('cosesmr')):15.2f}"
            todirect = f"{coalesce(row.get('todirect')):15.2f}"
            tocost   = f"{coalesce(row.get('tocost')):15.2f}"
            totfee   = f"{coalesce(row.get('totfee')):15.2f}"
            floamt   = f"{coalesce(row.get('floamt')):15.2f}"
            profit   = f"{coalesce(row.get('profit')):15.2f}"
            f.write(f"{custname};{tocnt};{hware};{datacomm};{cosesmr};"
                    f"{todirect};{tocost};{totfee};{floamt};{profit};\n")

# [Include the remaining output functions: write_print_report, write_costexp, write_missname_report]
# [These remain the same as the previous version]

# =============================================================================
# MAIN
# =============================================================================

def main():
    print("EIBMCOSR: Starting cost analysis processing...")
    print(f"Current working directory: {os.getcwd()}")
    print(f"Input directory: {INPUT_DIR}")
    
    # List available files
    if os.path.exists(INPUT_DIR):
        files = glob.glob(os.path.join(INPUT_DIR, "*.sas7bdat"))
        print(f"Found {len(files)} SAS7BDAT files")
    else:
        print(f"WARNING: Input directory not found: {INPUT_DIR}")

    rv = get_report_vars()
    print(f"Report date: {rv['rdate']}, Period: {rv['sdate']} to {rv['rdate']}, Months: {rv['smon']} to {rv['emon']}")

    # Load rate data
    rate = load_rate(rv['smon'], rv['emon'])
    print(f"Loaded rate data: {len(rate)} rows")

    # Process main transaction data
    totsum, missname, except_df = process_main(rv, rate)
    print(f"TOTSUM: {len(totsum)} rows")

    if totsum.is_empty():
        print("ERROR: No transaction data processed. Exiting.")
        return

    # Process ESMR
    esmr2, missname_esmr = process_esmr(rv, rate)
    if not missname_esmr.is_empty():
        missname = pl.concat([missname_esmr, missname], how='diagonal')

    # Process other costs
    othcost2, missname_oth = process_othercost(rv)
    if not missname_oth.is_empty():
        missname = pl.concat([missname_oth, missname], how='diagonal')

    # Process deposit balance
    dpbal, missname_bal = process_depbal(rv, rate)
    if not missname_bal.is_empty():
        missname = pl.concat([missname_bal, missname], how='diagonal')

    # Build grand total
    totsum_final = build_grand_total(totsum, esmr2, othcost2, dpbal, rv, rate)
    print(f"Final TOTSUM: {len(totsum_final)} rows")

    # Write outputs
    write_costxt01(totsum_final, COSTXT01_TXT)
    print(f"Written: {COSTXT01_TXT}")

    # write_print_report(totsum_final, rv, COSTXT02_TXT)
    # write_missname_report(missname, COSTXT02_TXT)
    # write_costexp(except_df, rv, COSTEXP_TXT)

    print("EIBMCOSR: Processing complete.")


if __name__ == '__main__':
    main()
