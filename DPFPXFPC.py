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
# Additionally, HP_ALL list is used for the HPSETTLE filter

from PBBLNFMT import put, HP_ALL

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
# &HPD MACRO — HP product codes sourced from PBBLNFMT.HP_ALL
# SAS: IF (LOANTYPE IN &HPD OR LOANTYPE IN (15,20,63,71,72)) AND PAIDIND='P'
# =============================================================================
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

# =============================================================================
# BUILD HPSNR — settled and released same month
# =============================================================================

def build_hpsnr(hpsettle: pl.DataFrame, sdate: date, bnm_loan: pl.DataFrame) -> pl.DataFrame:
    """
    DATA HPSNR: SET HPSETTLE;
      IF ISSDTE >= SDATE AND CLOSEDTE >= SDATE;
    PROC SORT; BY ACCTNO NOTENO;
    DATA HPSNR(KEEP=...): MERGE HPSNR(IN=A) BNM.LOAN<MM><WK>; BY ACCTNO NOTENO; IF A;
    """
    if hpsettle.is_empty():
        return pl.DataFrame()

    out_rows = []
    for row in hpsettle.to_dicts():
        issdte   = row.get('issdte')
        closedte = row.get('closedte')
        if issdte is None or closedte is None:
            continue
        if issdte >= sdate and closedte >= sdate:
            out_rows.append(row)

    if not out_rows:
        return pl.DataFrame()

    hpsnr = pl.from_dicts(out_rows).sort(['acctno','noteno'])

    if bnm_loan.is_empty():
        return hpsnr

    # MERGE HPSNR(IN=A) BNM.LOAN; BY ACCTNO NOTENO; IF A
    loan_keep = [c for c in ['acctno','noteno','fisspurp','product','noteterm',
                              'earnterm','netproc','apprdate','apprlim2','prodcd',
                              'custcd','amtind','sectorcd','balance','issdte',
                              'acctype'] if c in bnm_loan.columns]
    loan_sel  = bnm_loan.select(loan_keep)

    merged = hpsnr.join(loan_sel, on=['acctno','noteno'], how='left', suffix='_ln')
    for c in [x for x in loan_keep if x not in ('acctno','noteno')]:
        lc = f"{c}_ln"
        if lc in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(lc).is_not_null())
                  .then(pl.col(lc))
                  .otherwise(pl.col(c) if c in merged.columns else pl.lit(None))
                  .alias(c)
            ).drop(lc)

    return merged.select([c for c in ['acctno','noteno','fisspurp','product',
                                       'noteterm','earnterm','netproc','apprdate',
                                       'apprlim2','prodcd','custcd','amtind',
                                       'sectorcd','balance','issdte','acctype']
                          if c in merged.columns])

# =============================================================================
# BUILD HP — new HP accounts issued this month (not settled)
# =============================================================================

def build_hp_new(bnm_loan: pl.DataFrame, sdate: date) -> pl.DataFrame:
    """
    DATA HP:
      SET BNM.LOAN<MM><WK>;
      IF CURBAL > 0 AND PRODCD IN ('34111') AND ISSDTE >= SDATE;
    """
    if bnm_loan.is_empty():
        return pl.DataFrame()

    keep = [c for c in ['acctno','noteno','fisspurp','product','noteterm',
                         'earnterm','netproc','apprdate','apprlim2','prodcd',
                         'custcd','amtind','sectorcd','balance','curbal',
                         'issdte','acctype'] if c in bnm_loan.columns]
    df   = bnm_loan.select(keep)

    out_rows = []
    for row in df.to_dicts():
        curbal  = coalesce_f(row.get('curbal'))
        prodcd  = coalesce_s(row.get('prodcd'))
        issdte  = row.get('issdte')

        if isinstance(issdte, (int, float)):
            issdte = sas_date_to_pydate(issdte)
        elif isinstance(issdte, str):
            issdte = sas_date_to_pydate(issdte)
            
        if issdte is None:
            continue
        if curbal > 0 and prodcd == '34111' and issdte >= sdate:
            r2 = dict(row)
            r2['issdte'] = issdte
            out_rows.append(r2)

    if not out_rows:
        return pl.DataFrame()

    result = pl.from_dicts(out_rows)
    drop_cols = [c for c in ['curbal'] if c in result.columns]
    if drop_cols:
        result = result.drop(drop_cols)
    return result

# =============================================================================
# MERGE HPSETTLE + HP — split into HP, EXCEPT, then add HPSNR
# =============================================================================

def merge_hp_settle(hpsettle_df: pl.DataFrame, hp_new: pl.DataFrame,
                    hpsnr_df: pl.DataFrame) -> tuple:
    """
    DATA HPSETTLE(RENAME=...): rename columns for settle dataset
    PROC SORT HPSETTLE NODUPKEYS; BY ACCTNO;
    PROC SORT HPSNR NODUPKEYS; BY ACCTNO;
    PROC SORT HP; BY ACCTNO;
    DATA HP EXCEPT;
      MERGE HPSETTLE(IN=B) HP(IN=A); BY ACCTNO;
      IF (A AND B): OUTPUT EXCEPT;
        IF SAME MONTH issdte: OUTPUT HP;
      IF A AND NOT B: OUTPUT HP;
    DATA HPSNR: MERGE HPSNR(IN=B) HP(IN=A); BY ACCTNO; IF B AND NOT A;
    DATA HP: SET HP HPSNR;
    """
    rename_map = {
        'noteno':   'onote',
        'custcode': 'ocustcd',
        'sector':   'osector',
        'loantype': 'oprod',
        'netproc':  'onet',
        'issdte':   'oissdte',
        'crispurp': 'ofiss',
        'closedte': 'oclose',
    }
    if not hpsettle_df.is_empty():
        cols_to_rename = {k: v for k, v in rename_map.items()
                          if k in hpsettle_df.columns}
        settle = hpsettle_df.rename(cols_to_rename)
        settle = settle.unique(subset=['acctno'], keep='first').sort('acctno')
    else:
        settle = pl.DataFrame()

    if not hpsnr_df.is_empty():
        hpsnr_dedup = hpsnr_df.unique(subset=['acctno'], keep='first').sort('acctno')
    else:
        hpsnr_dedup = pl.DataFrame()

    if not hp_new.is_empty():
        hp_sorted = hp_new.sort('acctno')
    else:
        hp_sorted = pl.DataFrame()

    hp_rows     = []
    except_rows = []

    if not hp_sorted.is_empty():
        if not settle.is_empty():
            merged = hp_sorted.join(settle, on='acctno', how='left', suffix='_st')
            for row in merged.to_dicts():
                in_b  = row.get('onote') is not None
                issdte   = row.get('issdte')
                oissdte  = row.get('oissdte')

                if in_b:
                    except_rows.append(dict(row))
                    if (issdte is not None and oissdte is not None and
                            issdte.month == oissdte.month and
                            issdte.year  == oissdte.year):
                        hp_rows.append(dict(row))
                else:
                    hp_rows.append(dict(row))
        else:
            hp_rows = hp_sorted.to_dicts()

    hp_df     = pl.from_dicts(hp_rows)     if hp_rows     else pl.DataFrame()
    except_df = pl.from_dicts(except_rows) if except_rows else pl.DataFrame()

    # DATA HPSNR: MERGE HPSNR(IN=B) HP(IN=A); IF B AND NOT A
    final_hpsnr = pl.DataFrame()
    if not hpsnr_dedup.is_empty() and not hp_df.is_empty():
        hp_keys = hp_df.select('acctno').with_columns(pl.lit(True).alias('_in_a'))
        snr_m   = hpsnr_dedup.join(hp_keys, on='acctno', how='left')
        snr_m   = snr_m.filter(pl.col('_in_a').is_null()).drop('_in_a')
        final_hpsnr = snr_m
    elif not hpsnr_dedup.is_empty():
        final_hpsnr = hpsnr_dedup

    # DATA HP: SET HP HPSNR
    if not final_hpsnr.is_empty():
        hp_df = pl.concat([hp_df, final_hpsnr], how='diagonal') if not hp_df.is_empty() \
                else final_hpsnr

    return hp_df, except_df, final_hpsnr

# =============================================================================
# SECTOR MAPPING — $NEWSECT. and $VALIDSE. from PBBLNFMT
# =============================================================================

def apply_sectcd_mapping(rows: list) -> list:
    """
    DATA ALM:
       SECTA    = PUT(SECTORCD, $NEWSECT.);   -> put(sectorcd, 'NEWSECT')
       SECVALID = PUT(SECTORCD, $VALIDSE.);   -> put(sectorcd, 'VALIDSE')
       IF SECTA NE '    ' THEN SECTCD = SECTA;
       ELSE                    SECTCD = SECTORCD;
    Then remap SECTCD when SECVALID = 'INVALID'.
    """
    for row in rows:
        scd      = coalesce_s(row.get('sectorcd'))
        secta    = put(scd, 'NEWSECT', '')    # $NEWSECT. from PBBLNFMT
        secvalid = put(scd, 'VALIDSE', '')    # $VALIDSE. from PBBLNFMT
        row['secvalid'] = secvalid
        # IF SECTA NE '    ' THEN SECTCD=SECTA; ELSE SECTCD=SECTORCD;
        sectcd = secta if secta.strip() else scd
        # IF SECVALID='INVALID' THEN DO; remap SECTCD; END;
        if secvalid == 'INVALID':
            sectcd = remap_invalid_sectcd(sectcd)
        row['sectcd'] = sectcd
    return rows


def remap_invalid_sectcd(sectcd: str) -> str:
    """
    DATA ALM: IF SECVALID='INVALID' THEN DO;
    Map invalid sector codes to default codes based on first 1-2 chars.
    """
    p1 = sectcd[0:1] if sectcd else ''
    p2 = sectcd[0:2] if len(sectcd) >= 2 else ''

    if p1 == '1': return '1400'
    if p1 == '2': return '2900'
    if p1 == '3': return '3919'
    if p1 == '4': return '4010'
    if p1 == '5': return '5999'
    if p2 == '61': return '6120'
    if p2 == '62': return '6130'
    if p2 == '63': return '6310'
    if p2 in ('64','65','66','67','68','69'): return '6130'
    if p1 == '7': return '7199'
    if p2 in ('81','82'): return '8110'
    if p2 in ('83','84','85','86','87','88','89'): return '8999'
    if p2 == '91': return '9101'
    if p2 == '92': return '9410'
    if p2 in ('93','94','95'): return '9499'
    if p2 in ('96','97','98','99'): return '9999'
    return sectcd


# [Rest of the functions remain the same...]
