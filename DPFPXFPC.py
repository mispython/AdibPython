#!/usr/bin/env python3
"""
Program Name: EIBMHPFS.py
Purpose: HP FISS - Disbursement, Repayment, Approval reporting for BNM
         - Identifies newly issued / settled HP accounts from LOAN.LNNOTE
         - Merges with BNM.LOAN<MM><WK> for current balances
         - Builds RDAL fixed-width output file (LRECL=80) with BNM codes
           covering disbursement by: purpose code, custcd, sectorial code,
           count by custcd; with new sector mapping rollup
         - Produces EXCEPT and HPSNR exception/same-month release reports
           (ASA carriage-control, LRECL=133)

ESMR: 06-1485, 06-1762
"""

# %INC PGM(PBBLNFMT) — PBBLNFMT is a genuine dependency of this program.
# The put() function from PBBLNFMT is used for sector mapping:
#   $NEWSECT.  -> put(code, 'NEWSECT')  maps old sector codes to new codes
#   $VALIDSE.  -> put(code, 'VALIDSE')  returns 'VALID' or 'INVALID'

from PBBLNFMT import put

import os
import pyreadstat
from datetime import date, timedelta
from typing import Optional

import polars as pl

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

LOAN_LNNOTE_SAS7BDAT   = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBAABBA/lnnote.sas7bdat"
BNM_LOAN_PREFIX        = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMHPFS/ln{REPTMON}{NOWK}{REPTYEAR}.sas7bdat"    

OUTPUT_DIR             = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMHPFS"
RDAL_TXT               = os.path.join(OUTPUT_DIR, "hp_rdal.txt")
EXCEPT_TXT             = os.path.join(OUTPUT_DIR, "hp_except.txt")
HPSNR_TXT              = os.path.join(OUTPUT_DIR, "hp_snr.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# =============================================================================
# &HPD MACRO — HP product codes
# SAS: IF (LOANTYPE IN &HPD OR LOANTYPE IN (15,20,63,71,72)) AND PAIDIND='P'
# HP_ALL from SAS: [128,130,131,132,380,381,700,705,720,725,983,993,996,678,679,698,699]
# =============================================================================
HP_ALL = [128, 130, 131, 132, 380, 381, 700, 705, 720, 725, 
          983, 993, 996, 678, 679, 698, 699]
HPD_SET: set[int] = set(HP_ALL) | {15, 20, 63, 71, 72}

# =============================================================================
# PAGE / ASA CONSTANTS
# =============================================================================

PAGE_LENGTH = 60

# =============================================================================
# DATE / UTILITY HELPERS
# =============================================================================

def sas_date_to_pydate(val) -> Optional[date]:
    """Convert SAS date to Python date"""
    if val is None or (isinstance(val, float) and val != val):
        return None
    if isinstance(val, (int, float)):
        return date(1960, 1, 1) + timedelta(days=int(val))
    if isinstance(val, date):
        return val
    if isinstance(val, str):
        # Try to parse DDMMYY8 format
        try:
            if len(val) == 8:
                dd = int(val[0:2])
                mm = int(val[2:4])
                yy = int(val[4:6])
                # Assuming 20xx for years < 50, 19xx for >= 50
                if yy < 50:
                    yyyy = 2000 + yy
                else:
                    yyyy = 1900 + yy
                return date(yyyy, mm, dd)
        except (ValueError, TypeError):
            pass
    return None


def coalesce_s(val, default: str = '') -> str:
    return str(val).strip() if val is not None else default


def coalesce_i(val, default: int = 0) -> int:
    if val is None or (isinstance(val, float) and val != val):
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def coalesce_f(val, default: float = 0.0) -> float:
    if val is None or (isinstance(val, float) and val != val):
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def parse_z11_to_date(z11_val) -> Optional[date]:
    """
    INPUT(SUBSTR(PUT(val, Z11.), 1, 8), MMDDYY8.)
    PUT(val, Z11.) -> zero-padded 11-digit integer string
    SUBSTR(…,1,8) -> first 8 chars = MMDDYYYY
    INPUT(…, MMDDYY8.) -> parse as month/day/year
    """
    if z11_val is None or (isinstance(z11_val, float) and z11_val != z11_val):
        return None
    try:
        s = str(int(z11_val)).zfill(11)[:8]   # MMDDYYYY
        mm = int(s[0:2]); dd = int(s[2:4]); yy = int(s[4:8])
        if mm < 1 or mm > 12 or dd < 1 or dd > 31:
            return None
        return date(yy, mm, dd)
    except (ValueError, TypeError):
        return None


def fmt_date9(d: Optional[date]) -> str:
    """Format date as DATE9. = DDMonYYYY e.g. 01JAN2024"""
    if d is None:
        return '         '
    months = ['JAN','FEB','MAR','APR','MAY','JUN',
              'JUL','AUG','SEP','OCT','NOV','DEC']
    return f"{d.day:02d}{months[d.month-1]}{d.year:04d}"


def fmt_comma15_2(val) -> str:
    """Format as COMMA15.2"""
    if val is None or (isinstance(val, float) and val != val):
        return ' ' * 15
    v = float(val)
    s = f"{abs(v):,.2f}"
    if v < 0:
        s = '-' + s
    return s.rjust(15)

# =============================================================================
# REPORT DATE VARIABLES
# =============================================================================

def get_report_vars() -> dict:
    """
    Using datetime.timedelta(days=1) to get previous day as report date
    """
    # Get yesterday's date
    reptdate = date.today() - timedelta(days=1)
    day      = reptdate.day

    if day == 8:
        wk = '1'; wk1 = '4'
    elif day == 15:
        wk = '2'; wk1 = '1'
    elif day == 22:
        wk = '3'; wk1 = '2'
    else:
        wk = '4'; wk1 = '3'

    mm  = reptdate.month
    mm1 = mm - 1
    yy1 = reptdate.year
    if mm1 == 0:
        mm1 = 12
        yy1 -= 1

    sdate   = date(reptdate.year, mm,  1)
    psdate  = date(yy1,           mm1, 1)

    reptmon  = str(mm).zfill(2)
    reptmon1 = str(mm1).zfill(2)
    reptyear = str(reptdate.year)
    reptday  = str(day).zfill(2)
    rdate    = reptdate.strftime('%d/%m/%y')
    sdate_s  = sdate.strftime('%d/%m/%y')
    psdate_s = psdate.strftime('%d/%m/%y')

    return {
        'reptdate':  reptdate,
        'sdate':     sdate,
        'psdate':    psdate,
        'wk':        wk,
        'wk1':       wk1,
        'reptmon':   reptmon,
        'reptmon1':  reptmon1,
        'reptyear':  reptyear,
        'reptday':   reptday,
        'rdate':     rdate,
        'sdate_s':   sdate_s,
        'psdate_s':  psdate_s,
        'nowk':      wk,
        'nowk1':     wk1,
    }

# =============================================================================
# LOAD SAS7BDAT FILES
# =============================================================================

def load_sas7bdat_lowercase(path: str) -> pl.DataFrame:
    """
    Read SAS7BDAT file using pyreadstat and convert column names to lowercase
    """
    if not os.path.exists(path):
        print(f"Warning: File not found: {path}")
        return pl.DataFrame()
    
    try:
        df, meta = pyreadstat.read_sas7bdat(path)
        # Convert column names to lowercase
        df.columns = [col.lower() for col in df.columns]
        return pl.from_pandas(df)
    except Exception as e:
        print(f"Error reading {path}: {e}")
        return pl.DataFrame()

# =============================================================================
# LOAD BNM LOAN
# =============================================================================

def load_bnm_loan(rv: dict) -> pl.DataFrame:
    path = (BNM_LOAN_PREFIX
            .replace('{REPTMON}', rv['reptmon'])
            .replace('{NOWK}', rv['nowk'])
            .replace('{REPTYEAR}', rv['reptyear']))
    return load_sas7bdat_lowercase(path)

# =============================================================================
# BUILD HPSETTLE — settled HP accounts from LOAN.LNNOTE
# =============================================================================

def build_hpsettle(rv: dict) -> pl.DataFrame:
    """
    DATA HPSETTLE(KEEP=ACCTNO NOTENO CUSTCODE SECTOR LOANTYPE
       NETPROC ISSDTE CRISPURP CLOSEDTE);
       SET LOAN.LNNOTE;
       IF (LOANTYPE IN &HPD OR LOANTYPE IN (15,20,63,71,72)) AND PAIDIND='P';
       CLOSEDTE = INPUT(SUBSTR(PUT(LASTTRAN,Z11.),1,8), MMDDYY8.);
       ISSDTE   = INPUT(SUBSTR(PUT(ISSUEDT, Z11.),1,8), MMDDYY8.);
    """
    df = load_sas7bdat_lowercase(LOAN_LNNOTE_SAS7BDAT)

    if df.is_empty():
        return pl.DataFrame()

    keep = ['acctno','noteno','custcode','sector','loantype',
            'netproc','crispurp','lasttran','issuedt','paidind']
    avail = [c for c in keep if c in df.columns]
    df    = df.select(avail)

    rows     = df.to_dicts()
    out_rows = []
    for row in rows:
        loantype = coalesce_i(row.get('loantype'))
        paidind  = coalesce_s(row.get('paidind'))
        if loantype not in HPD_SET:
            continue
        if paidind != 'P':
            continue
        closedte = parse_z11_to_date(row.get('lasttran'))
        issdte   = parse_z11_to_date(row.get('issuedt'))
        out = {
            'acctno':   row.get('acctno'),
            'noteno':   row.get('noteno'),
            'custcode': coalesce_s(row.get('custcode')),
            'sector':   coalesce_s(row.get('sector')),
            'loantype': loantype,
            'netproc':  coalesce_f(row.get('netproc')),
            'crispurp': coalesce_s(row.get('crispurp')),
            'issdte':   issdte,
            'closedte': closedte,
        }
        out_rows.append(out)

    return pl.from_dicts(out_rows) if out_rows else pl.DataFrame()

# [Rest of the code remains the same...]
