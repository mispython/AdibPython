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

# Input SAS7BDAT paths (COST library)
COST_NAME_SAS7BDAT      = "input/cost/name.sas7bdat"
COST_ESMR_SAS7BDAT      = "input/cost/esmr.sas7bdat"
COST_OTHCOST_SAS7BDAT   = "input/cost/othcost.sas7bdat"

# Monthly data SAS7BDAT prefix patterns (e.g. cost/rate01.sas7bdat .. rate12.sas7bdat)
COST_RATE_PREFIX       = "input/cost/rate"        # rate01..rate12.sas7bdat
COST_STBK_PREFIX       = "input/cost/stbk"        # stbk01..stbk12.sas7bdat
COST_DPCOL_PREFIX      = "input/cost/dpcol"       # dpcol01..dpcol12.sas7bdat
COST_DPECP_PREFIX      = "input/cost/dpecp"       # dpecp01..dpecp12.sas7bdat
COST_DPDDS_PREFIX      = "input/cost/dpdds"       # dpdds01..dpdds12.sas7bdat
COST_DPMISC_PREFIX     = "input/cost/dpmisc"      # dpmisc01..dpmisc12.sas7bdat
COST_DPBAL_PREFIX      = "input/cost/dpbal"       # dpbal01..dpbal12.sas7bdat

# Output paths
COSTXT01_TXT  = "output/cost01_text.txt"           # SAP.PBB.COST01.TEXT
COSTXT02_TXT  = "output/cost02_text.txt"           # SAP.PBB.COST02.TEXT  (SASLIST / PRINT report)
COSTEXP_TXT   = "output/cost_except_text.txt"      # SAP.PBB.COST.EXCEPT.TEXT

os.makedirs("output", exist_ok=True)

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

def fmt_comma15_2(val):
    return fmt_comma(val, 15, 2)

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

# =============================================================================
# SAS7BDAT READER HELPER
# =============================================================================

def read_sas7bdat(filepath: str) -> pl.DataFrame:
    """Read a SAS7BDAT file using pyreadstat and return a Polars DataFrame."""
    if not os.path.exists(filepath):
        print(f"  WARNING: File not found: {filepath}")
        return pl.DataFrame()
    try:
        df, meta = pyreadstat.read_sas7bdat(filepath)
        if df.empty:
            print(f"  WARNING: Empty file: {filepath}")
            return pl.DataFrame()
        print(f"  Loaded {filepath}: {len(df)} rows, columns: {list(df.columns)}")
        return pl.from_pandas(df)
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
    # *** SRPTDATE = MDY(MONTH(REPTDATE),1,YEAR(REPTDATE))
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
# MONTHLY DATA LOADER (replaces %MACRO loops with PROC APPEND)
# =============================================================================

def load_monthly_sas7bdat(prefix: str, smon: int, emon: int) -> pl.DataFrame:
    """
    Load and union monthly SAS7BDAT files from smon to emon.
    Equivalent to the %DO I=&SMON %TO &EMON macro loops with PROC APPEND.
    """
    frames = []
    for i in range(smon, emon + 1):
        month_str = str(i).zfill(2)
        path = f"{prefix}{month_str}.sas7bdat"
        print(f"  Looking for: {path}")
        if os.path.exists(path):
            df = read_sas7bdat(path)
            if not df.is_empty():
                frames.append(df)
        else:
            print(f"  File not found: {path}")
    
    if not frames:
        print(f"  WARNING: No files found for prefix {prefix}, months {smon}-{emon}")
        return pl.DataFrame()
    
    result = pl.concat(frames, how='diagonal')
    print(f"  Combined {len(frames)} files for {prefix}: {len(result)} total rows")
    return result

# =============================================================================
# %COSTRT — Load and sort RATE data
# =============================================================================

def load_rate(smon: int, emon: int) -> pl.DataFrame:
    """Load all rate monthly SAS7BDAT files, sort BY TRANDT."""
    rate = load_monthly_sas7bdat(COST_RATE_PREFIX, smon, emon)
    if rate.is_empty():
        print("  WARNING: No rate data loaded!")
        return rate
    
    # Convert TRANDT to proper date if it's numeric
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
        elif dtype == pl.Date:
            pass  # Already date type
        else:
            print(f"  WARNING: Unexpected TRANDT dtype: {dtype}")
    
    return rate.sort('trandt')

# =============================================================================
# %PROCESS — Main transaction processing
# =============================================================================

def process_main(rv: dict, rate: pl.DataFrame) -> tuple:
    """
    Implements %PROCESS macro:
    - Load STBK, DPCOL, DPECP, DPDDS, DPMISC
    - Build TOTSUM, compute costs per SVTYPE
    - Merge with NAME
    - PROC MEANS by NMABBR
    Returns: totsum (pl.DataFrame), missname (pl.DataFrame), except_df (pl.DataFrame)
    """
    smon, emon = rv['smon'], rv['emon']
    noofday    = rv['noofday']
    reptyear   = int(rv['reptyear'])

    # *** 1. STOCK BANKING ***
    stbk  = load_monthly_sas7bdat(COST_STBK_PREFIX,  smon, emon)
    # *** 2. DEPOSIT COLLECTION ***
    dpcol = load_monthly_sas7bdat(COST_DPCOL_PREFIX, smon, emon)
    # *** 3. DEPOSIT ECP/FDS ***
    dpecp = load_monthly_sas7bdat(COST_DPECP_PREFIX, smon, emon)
    # *** 4. DEPOSIT DDS ***
    dpdds = load_monthly_sas7bdat(COST_DPDDS_PREFIX, smon, emon)
    # *** 5. DEPOSIT MISC ***
    dpmisc = load_monthly_sas7bdat(COST_DPMISC_PREFIX, smon, emon)

    # DATA TOTSUM: SET STBK DPCOL DPECP DPDDS DPMISC;
    all_frames = [stbk, dpcol, dpecp, dpdds, dpmisc]
    non_empty = [df for df in all_frames if not df.is_empty()]
    
    if not non_empty:
        print("  ERROR: No transaction data loaded!")
        return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()
    
    totsum = pl.concat(non_empty, how='diagonal')
    print(f"  TOTSUM before processing: {len(totsum)} rows, columns: {totsum.columns}")
    
    # Check if TRANDT exists
    if 'trandt' not in totsum.columns:
        print("  ERROR: TRANDT column not found in transaction data!")
        print(f"  Available columns: {totsum.columns}")
        return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()
    
    # Convert TRANDT to date if needed
    dtype = totsum['trandt'].dtype
    print(f"  TOTSUM TRANDT dtype: {dtype}")
    if dtype in [pl.Float64, pl.Int64, pl.Float32, pl.Int32]:
        totsum = totsum.with_columns(
            pl.col('trandt').map_elements(
                lambda v: sas_date_to_pydate(v) if v is not None else None,
                return_dtype=pl.Date
            )
        )
    
    totsum = totsum.sort('trandt')

    # DATA EXCEPT: rows where SVTYPE not in the known list
    known_svtypes = {'FDSIBG','FDSIBK','ECP','DDS','EBK','TEL','ATM','OTC','ESI',
                     'PREATM','PRESMS','FPXCOL','FPXPYM','STM','PREEBK','MTN',
                     'MIS','PAG','CDT','CDM','MBK'}
    
    if 'svtype' in totsum.columns:
        except_df = totsum.filter(~pl.col('svtype').is_in(list(known_svtypes)))
    else:
        except_df = pl.DataFrame()

    # MERGE TOTSUM + RATE BY TRANDT (left join, keep IF A)
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

    # Compute CNT/COS columns per SVTYPE
    cnt_cos_cols = [
        'cntfds','cosfds','cntecp','cosecp','cntdds','cosdds','cntebk','cosebk',
        'cnttel','costel','cntatm','cosatm','cntotc','cosotc','cntesi','cosesi',
        'cntmisc','cosmisc','cntfpxco','cosfpxco','cntfpxpy','cosfpxpy',
        'cntpag','cospag','cntcdt','coscdt','cntcdm','coscdm','cntmbk','cosmbk'
    ]
    # Initialize all cnt/cos columns to null
    for c in cnt_cos_cols:
        if c not in totsum.columns:
            totsum = totsum.with_columns(pl.lit(None).cast(pl.Float64).alias(c))

    # Apply row-wise SVTYPE logic
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

    # TOTFEE = SUM(FEEAMT1, FEEAMT2)
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

    # Sort TOTSUM BY ACCTNO
    if 'acctno' in totsum.columns:
        totsum = totsum.sort('acctno')

    # *** A/C NOT FOUND IN CONTROL FILE (ESMR) ***
    missname = pl.DataFrame()
    if not name_df.is_empty() and 'acctno' in totsum.columns:
        missname = totsum.join(name_df, on='acctno', how='anti')

    # MERGE TOTSUM + NAME BY ACCTNO, keep IF A
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

    # PROC MEANS BY NMABBR SUM -> aggregate all cnt/cos + totfee columns
    agg_cols = cnt_cos_cols + ['totfee']
    available_agg = [c for c in agg_cols if c in totsum.columns]
    
    if not totsum.is_empty() and 'nmabbr' in totsum.columns:
        totsum = totsum.sort('nmabbr').group_by('nmabbr', maintain_order=True).agg(
            [pl.col(c).sum().alias(c) for c in available_agg] +
            ([pl.col('custname').first().alias('custname')] if 'custname' in totsum.columns else [])
        )

    return totsum, missname, except_df

# =============================================================================
# %ESMRDTL — ESMR detail processing
# =============================================================================

def process_esmr(rv: dict, rate: pl.DataFrame) -> tuple:
    """
    Implements %ESMRDTL macro.
    Returns: esmr2 (pl.DataFrame), missname_esmr (pl.DataFrame)
    """
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

    # IF EXPDT GE SDATE
    if 'expdt' in esmr_raw.columns:
        esmr_raw = esmr_raw.filter(pl.col('expdt') >= sdate_d)

    # MERGE ESMR + RATE BY TRANDT (left join)
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

    # THISDATE = SDATE; MTHCOUNT = months between EXPDT and THISDATE + 1
    # COSESMR = (MANDAY * MANRATE) / 60
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

        # %DO I = &SMON %TO &EMON: filter MTHCOUNT GE I and append
        frames = []
        for i in range(smon, emon + 1):
            subset = esmr_merged.filter(pl.col('mthcount') >= i)
            frames.append(subset)
        esmr2 = pl.concat(frames, how='diagonal') if frames else pl.DataFrame()
    else:
        esmr2 = esmr_merged

    # Load NAME dataset
    name_df = read_sas7bdat(COST_NAME_SAS7BDAT)
    if not name_df.is_empty():
        name_df = name_df.unique(subset=['acctno'], keep='first').sort('acctno')

    if name_df.is_empty() or 'acctno' not in esmr2.columns:
        return esmr2, pl.DataFrame()

    esmr2 = esmr2.sort('acctno')

    # *** A/C NOT FOUND IN CONTROL FILE (ESMR) ***
    missacc = esmr2.join(name_df, on='acctno', how='anti')

    # Keep only A AND B (inner join)
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

    # PROC MEANS BY NMABBR SUM COSESMR
    if not esmr2.is_empty() and 'nmabbr' in esmr2.columns and 'cosesmr' in esmr2.columns:
        esmr2 = esmr2.sort('nmabbr').group_by('nmabbr', maintain_order=True).agg(
            pl.col('cosesmr').sum()
        )

    return esmr2, missacc

# =============================================================================
# %OTHERDTL — Other cost processing (hardware + datacomm)
# =============================================================================

def process_othercost(rv: dict) -> tuple:
    """
    Implements %OTHERDTL macro.
    Returns: othcost2 (pl.DataFrame), missname_oth (pl.DataFrame)
    """
    smon    = rv['smon']
    emon    = rv['emon']
    sdate_d = parse_ddmmyy8(rv['sdate'])
    rdate_d = parse_ddmmyy8(rv['rdate'])

    othcost_raw = read_sas7bdat(COST_OTHCOST_SAS7BDAT)
    if othcost_raw.is_empty():
        print("  WARNING: No other cost data found")
        return pl.DataFrame(), pl.DataFrame()

    # Convert dates from SAS numeric to Python date
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

    # Filter: CTYPE='DC' OR (CTYPE='HW' AND EXPDT GE SDATE AND TRANDT LE RDATE)
    if 'ctype' in othcost_raw.columns and 'expdt' in othcost_raw.columns and 'trandt' in othcost_raw.columns:
        othcost = othcost_raw.filter(
            (pl.col('ctype') == 'DC') |
            (
                (pl.col('ctype') == 'HW') &
                (pl.col('expdt')  >= sdate_d) &
                (pl.col('trandt') <= rdate_d)
            )
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

        # %DO I = &SMON %TO &EMON: filter MTHCOUNT GE I OR CTYPE='DC'
        frames = []
        for i in range(smon, emon + 1):
            subset = othcost.filter(
                (pl.col('mthcount') >= i) | (pl.col('ctype') == 'DC')
            )
            frames.append(subset)
        othcost2 = pl.concat(frames, how='diagonal') if frames else pl.DataFrame()
    else:
        othcost2 = othcost

    # NAME lookup
    name_df = read_sas7bdat(COST_NAME_SAS7BDAT)
    if not name_df.is_empty():
        name_df = name_df.unique(subset=['acctno'], keep='first').sort('acctno')

    if name_df.is_empty() or 'acctno' not in othcost2.columns:
        return othcost2, pl.DataFrame()

    othcost2 = othcost2.sort('acctno')

    # *** A/C NOT FOUND IN CONTROL FILE (ESMR) ***
    missacc = othcost2.join(name_df, on='acctno', how='anti')

    # A AND B
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

    # PROC MEANS BY NMABBR SUM HWARE DATACOMM
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
    """
    Implements %DEPBAL macro.
    Returns: dpbal (pl.DataFrame), missname_bal (pl.DataFrame)
    """
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

    # MERGE DPBAL + RATE (KEEP=TRANDT FLOAT) BY TRANDT
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

    # Leap year check
    dayyr = 366 if (reptyear % 4 == 0) else 365

    # FLOAMT calculation
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

    # NAME lookup
    name_df = read_sas7bdat(COST_NAME_SAS7BDAT)
    if not name_df.is_empty():
        name_df = name_df.unique(subset=['acctno'], keep='first').sort('acctno')

    if name_df.is_empty() or 'acctno' not in dpbal.columns:
        return dpbal, pl.DataFrame()

    dpbal = dpbal.sort('acctno')

    # *** A/C NOT FOUND IN CONTROL FILE (ESMR) ***
    missacc = dpbal.join(name_df, on='acctno', how='anti')

    # A AND B
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

    # PROC MEANS BY NMABBR SUM FLOAMT
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
    """
    Implements the GRAND TOTAL DATA step and final null-to-zero replacements.
    """
    if totsum.is_empty():
        print("  WARNING: TOTSUM is empty, cannot build grand total")
        return pl.DataFrame()
        
    rdate_d = parse_ddmmyy8(rv['rdate'])

    # Load NAME sorted by NMABBR
    name_df = read_sas7bdat(COST_NAME_SAS7BDAT)
    if not name_df.is_empty():
        name_df = name_df.unique(subset=['nmabbr'], keep='first').sort('nmabbr')
        name_df = name_df.select(['custname', 'nmabbr'])

    # Prepare join frames (select only needed columns)
    esmr_join    = esmr2.select(['nmabbr','cosesmr'])    if not esmr2.is_empty()    and 'cosesmr'  in esmr2.columns    and 'nmabbr' in esmr2.columns else pl.DataFrame()
    dpbal_join   = dpbal.select(['nmabbr','floamt'])     if not dpbal.is_empty()    and 'floamt'   in dpbal.columns    and 'nmabbr' in dpbal.columns else pl.DataFrame()
    othcost_cols = ['nmabbr'] + [c for c in ['hware','datacomm'] if c in othcost2.columns]
    othcost_join = othcost2.select(othcost_cols)         if not othcost2.is_empty() and 'nmabbr' in othcost2.columns else pl.DataFrame()

    # MERGE TOTSUM + ESMR2 + OTHCOST2 + DPBAL + NAME BY NMABBR
    merged = totsum
    if not esmr_join.is_empty():
        merged = merged.join(esmr_join, on='nmabbr', how='outer', suffix='_esmr')
    if not othcost_join.is_empty():
        merged = merged.join(othcost_join, on='nmabbr', how='left', suffix='_oth')
    if not dpbal_join.is_empty():
        merged = merged.join(dpbal_join, on='nmabbr', how='left', suffix='_bal')
    if not name_df.is_empty():
        merged = merged.join(name_df, on='nmabbr', how='left', suffix='_name')

    # Resolve cosesmr
    if 'cosesmr_esmr' in merged.columns:
        merged = merged.with_columns(
            pl.when(pl.col('cosesmr_esmr').is_not_null())
              .then(pl.col('cosesmr_esmr'))
              .otherwise(pl.col('cosesmr') if 'cosesmr' in merged.columns else pl.lit(None))
              .alias('cosesmr')
        ).drop('cosesmr_esmr')

    # Resolve custname
    if 'custname_name' in merged.columns:
        merged = merged.with_columns(
            pl.when(pl.col('custname_name').is_not_null())
              .then(pl.col('custname_name'))
              .otherwise(pl.col('custname') if 'custname' in merged.columns else pl.lit(None))
              .alias('custname')
        ).drop('custname_name')

    # Compute TODIRECT, TOCNT, TOCOST, PROFIT
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

    # TRANDT = RDATE
    merged = merged.with_columns(pl.lit(rdate_d).alias('trandt'))

    # MERGE TOTSUM + RATE BY TRANDT (left join)
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

    # Replace nulls with 0 for all numeric report columns
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
# OUTPUT FUNCTIONS (same as before)
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

# [Rest of the output functions remain the same - write_print_report, write_costexp, write_missname_report]

# ... [Include the remaining output functions from the previous version] ...

# =============================================================================
# MAIN
# =============================================================================

def main():
    print("EIBMCOSR: Starting cost analysis processing...")
    print(f"Current working directory: {os.getcwd()}")
    
    # List available files in input/cost directory
    if os.path.exists("input/cost"):
        files = glob.glob("input/cost/*.sas7bdat")
        print(f"Found {len(files)} SAS7BDAT files in input/cost/")
        for f in sorted(files):
            print(f"  {f}")
    else:
        print("WARNING: input/cost directory not found!")

    # Get report date variables (using datetime.timedelta - 1)
    rv = get_report_vars()
    print(f"  Report date: {rv['rdate']}, Period: {rv['sdate']} to {rv['rdate']}, "
          f"Months: {rv['smon']} to {rv['emon']}")

    # *** COST RATE ***
    rate = load_rate(rv['smon'], rv['emon'])
    print(f"  Loaded rate data: {len(rate)} rows")
    if not rate.is_empty():
        print(f"  Rate columns: {rate.columns}")

    # *** MAIN PROCESS ***
    totsum, missname, except_df = process_main(rv, rate)
    print(f"  TOTSUM (after PROCESS): {len(totsum)} rows")
    if not totsum.is_empty():
        print(f"  TOTSUM columns: {totsum.columns}")

    # Continue only if we have data
    if totsum.is_empty():
        print("ERROR: No transaction data processed. Cannot continue.")
        return

    # *** ESMR ***
    esmr2, missname_esmr = process_esmr(rv, rate)
    print(f"  ESMR2: {len(esmr2)} rows")

    # DATA MISSNAME: SET MISSACC MISSNAME (append)
    if not missname_esmr.is_empty():
        missname = pl.concat([missname_esmr, missname], how='diagonal')

    # *** OTHER COST ***
    othcost2, missname_oth = process_othercost(rv)
    print(f"  OTHCOST2: {len(othcost2)} rows")

    if not missname_oth.is_empty():
        missname = pl.concat([missname_oth, missname], how='diagonal')

    # *** TO CALCULATE FLOAT AMT ***
    dpbal, missname_bal = process_depbal(rv, rate)
    print(f"  DPBAL: {len(dpbal)} rows")

    if not missname_bal.is_empty():
        missname = pl.concat([missname_bal, missname], how='diagonal')

    # *** GRAND TOTAL ***
    totsum_final = build_grand_total(totsum, esmr2, othcost2, dpbal, rv, rate)
    print(f"  TOTSUM final: {len(totsum_final)} rows")

    # *** OUTPUT TEXT FILE (COSTXT01) ***
    write_costxt01(totsum_final, COSTXT01_TXT)
    print(f"  Written: {COSTXT01_TXT}")

    # *** REPORT FORMAT (SASLIST / COSTXT02) ***
    write_print_report(totsum_final, rv, COSTXT02_TXT)
    print(f"  Written report: {COSTXT02_TXT}")

    # *** MISSING OF A/C NUMBER ***
    write_missname_report(missname, COSTXT02_TXT)
    print(f"  Appended missing names to: {COSTXT02_TXT}")

    # *** EXCEPTION OF TRANSACTION TYPE ***
    write_costexp(except_df, rv, COSTEXP_TXT)
    print(f"  Written exception report: {COSTEXP_TXT}")

    print("EIBMCOSR: Processing complete.")


if __name__ == '__main__':
    main()
