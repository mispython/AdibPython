#!/usr/bin/env python3
"""
Program Name: RDL2PBIF.py
Purpose:      Process PBIF (Public Bank Invoice Financing) factoring loan data.

Dependencies / exports
----------------------
  build_pbif()        -> main builder
  format_fisstype()   -> SAS $FISSTYPE port (SECTFISS sub-sector label)
  format_fissgroup()  -> SAS $FISSGROUP port (SECTOR group label)
"""

import os
from datetime import date, datetime, timedelta
from typing import Optional

import pyreadstat
import polars as pl

try:
    import saspy
except ImportError:
    saspy = None


# =============================================================================
# PATH CONFIGURATION
# =============================================================================

PBIF_CLIEN_DIR    = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/RDL2PBIF"
MECHRG_TXT        = f"{PBIF_CLIEN_DIR}/mechrg.txt"
PBIF_OUTPUT_DIR   = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/RDL2PBIF"
PBIF_OUTPUT_NAME  = "pbif_output.sas7bdat"


# =============================================================================
# SAS FORMAT PORTS
# =============================================================================

def format_fisstype(sectorcd) -> str:
    """
    SAS $FISSTYPE equivalent — the SECTFISS 4-digit sub-sector code.

    The raw SECTORCD is already a 4-digit code like '1100', '1111', '3417',
    so we just normalise the string form. If it arrives as a number
    (e.g. 1100.0), strip the fractional part.
    """
    if sectorcd is None:
        return ''
    s = str(sectorcd).strip()
    if not s:
        return ''
    try:
        s = f"{int(float(s)):04d}"
    except (ValueError, TypeError):
        pass
    return s


def format_fissgroup(sectorcd) -> str:
    """
    SAS $FISSGROUP equivalent — the 4-digit SECTOR GROUP code.

    The group is the first two digits of the sub-sector code followed by
    '00'. Examples:
        '1100'  -> '1000'
        '1111'  -> '1000'
        '1200'  -> '1000'
        '2100'  -> '2000'
        '3110'  -> '3000'
        '34170' -> '34000'  (falls through as-is when >4 digits)
    """
    if sectorcd is None:
        return ''
    s = str(sectorcd).strip()
    if not s:
        return ''
    try:
        s = f"{int(float(s)):04d}"
    except (ValueError, TypeError):
        pass

    if len(s) >= 4:
        return s[:2] + '00'
    s = s.zfill(4)
    return s[:2] + '00'


# =============================================================================
# DATE HELPERS
# =============================================================================

def get_report_date() -> date:
    """Last day of previous month — aligned with EIMBNM01."""
    today = date.today()
    return today.replace(day=1) - timedelta(days=1)


def report_date_parts(d: date) -> dict:
    return {
        "reptyear": f"{d.year:04d}",
        "reptmon":  f"{d.month:02d}",
        "reptday":  f"{d.day:02d}",
    }


def sas_date_to_pydate(val) -> Optional[date]:
    if val is None or (isinstance(val, float) and val != val):
        return None
    if isinstance(val, (int, float)):
        return date(1960, 1, 1) + timedelta(days=int(val))
    if isinstance(val, date):
        return val
    return None


def pydate_to_sasdate(d: date) -> int:
    return (d - date(1960, 1, 1)).days


# =============================================================================
# %MACRO DCLVAR / NXTBLDT
# =============================================================================

def make_lday(year: int) -> list:
    lday = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    lday[2] = 29 if (year % 4 == 0) else 28
    return lday


def nxtbldt(matdte: date, freq: int, lday: list) -> date:
    dd = matdte.day
    mm = matdte.month + freq
    yy = matdte.year
    if mm > 12:
        mm -= 12
        yy += 1
    lday_local = list(lday)
    if mm == 2:
        lday_local[2] = 29 if (yy % 4 == 0) else 28
    if dd > lday_local[mm]:
        dd = lday_local[mm]
    return date(yy, mm, dd)


# =============================================================================
# LOAD CLIENT
# =============================================================================

def load_clien(clien_path: str) -> pl.DataFrame:
    if not os.path.exists(clien_path):
        return pl.DataFrame()

    df, _meta = pyreadstat.read_sas7bdat(clien_path)
    if df is None or df.empty:
        return pl.DataFrame()

    df.columns = [c.upper() for c in df.columns]

    if 'ENTITY' in df.columns:
        df = df[df['ENTITY'].astype(str).str.strip() == 'PBBH']

    if df.empty:
        return pl.DataFrame()

    return pl.from_pandas(df)


# =============================================================================
# LOAD MECHRG
# =============================================================================

def _parse_informat_12_2(raw: str) -> float:
    s = raw.strip()
    if not s:
        return 0.0
    try:
        if '.' in s:
            return float(s)
        return float(s) / 100.0
    except ValueError:
        return 0.0


def _parse_yymmdd8(s: str) -> Optional[date]:
    s = s.strip()
    if len(s) < 6:
        return None
    try:
        yy_ = int(s[0:2])
        mm_ = int(s[2:4])
        dd_ = int(s[4:6])
        year_ = (2000 + yy_) if yy_ < 50 else (1900 + yy_)
        return date(year_, mm_, dd_)
    except (ValueError, IndexError):
        return None


def load_mechrg(mdate_int: int) -> pl.DataFrame:
    empty = pl.DataFrame(schema={'CLIENTNO': pl.Utf8, 'INTVAL': pl.Float64})
    if not os.path.exists(MECHRG_TXT):
        return empty

    rows = []
    with open(MECHRG_TXT, 'r', encoding='latin-1') as f:
        for line in f:
            if len(line) < 48:
                continue
            try:
                clientno  = line[0:9].strip()
                pdate_str = line[9:17]
                uval1_str = line[19:31]
                uval2_str = line[33:45]
                uval3_str = line[47:59]

                pdate = _parse_yymmdd8(pdate_str)
                if pdate is None:
                    continue
                if pydate_to_sasdate(pdate) != mdate_int:
                    continue

                intval = (_parse_informat_12_2(uval1_str) +
                          _parse_informat_12_2(uval2_str) +
                          _parse_informat_12_2(uval3_str))
                rows.append({'CLIENTNO': clientno, 'INTVAL': intval})
            except (ValueError, IndexError):
                continue

    if not rows:
        return empty

    df = pl.from_dicts(rows)
    return df.group_by('CLIENTNO').agg(pl.col('INTVAL').sum())


# =============================================================================
# CUSTFISS RECLASSIFICATION
# =============================================================================

def reclassify_custfiss(custfiss: str) -> str:
    if custfiss in ('41', '42', '43', '66'):
        return '41'
    if custfiss in ('44', '47', '67'):
        return '44'
    if custfiss == '46':
        return '46'
    if custfiss in ('48', '49', '51', '68'):
        return '48'
    if custfiss in ('52', '53', '54', '69'):
        return '52'
    return custfiss


# =============================================================================
# BUILD PBIF
# =============================================================================

def build_pbif(reptdate: Optional[date] = None) -> pl.DataFrame:
    if reptdate is None:
        reptdate = get_report_date()

    parts    = report_date_parts(reptdate)
    reptyear = parts["reptyear"]
    reptmon  = parts["reptmon"]
    reptday  = parts["reptday"]
    mdate_int = pydate_to_sasdate(reptdate)

    clien_path = os.path.join(
        PBIF_CLIEN_DIR, f"clien{reptyear}{reptmon}{reptday}.sas7bdat"
    )
    pbif = load_clien(clien_path)
    if pbif.is_empty():
        return pl.DataFrame()

    rows = pbif.to_dicts()
    for row in rows:
        custcd  = str(row.get('CUSTCD') or '').strip()
        inlimit = float(row.get('INLIMIT') or 0.0)
        row['APPRLIMX'] = inlimit
        row['PRODCD']   = '30591'
        row['FISSPURP'] = '0470'
        row['AMTIND']   = 'D'
        custfiss        = reclassify_custfiss(custcd)
        row['CUSTFISS'] = custfiss
        row['CUSTCX']   = custfiss
    pbif = pl.from_dicts(rows).sort('CLIENTNO')

    mechrg_df = load_mechrg(mdate_int)
    if not mechrg_df.is_empty():
        pbif = pbif.join(mechrg_df, on='CLIENTNO', how='left')
    elif 'INTVAL' not in pbif.columns:
        pbif = pbif.with_columns(pl.lit(None).cast(pl.Float64).alias('INTVAL'))

    out_rows = []
    for row in pbif.to_dicts():
        fiu      = float(row.get('FIU')      or 0.0)
        prmthfiu = float(row.get('PRMTHFIU') or 0.0)
        if fiu == 0.0 and prmthfiu == 0.0:
            continue
        intval_raw = row.get('INTVAL')
        intval = (0.0 if intval_raw is None or
                  (isinstance(intval_raw, float) and intval_raw != intval_raw)
                  else float(intval_raw))
        row['INTVAL'] = intval
        fiu = fiu + intval + prmthfiu

        balance  = fiu
        ufiu     = 0.0
        disburse = 0.0
        repaid   = 0.0
        rollover = 0.0

        if balance  < 0.0: balance  = 0.0
        if fiu      < 0.0: ufiu     = fiu
        if prmthfiu < 0.0: prmthfiu = 0.0

        if balance >= 0.0:
            if balance > prmthfiu:
                disburse = balance - prmthfiu
            else:
                repaid = prmthfiu - balance

        inlimit = float(row.get('INLIMIT') or 0.0)
        undrawn = inlimit - balance

        row.update({
            'FIU': fiu, 'BALANCE': balance, 'UFIU': ufiu,
            'DISBURSE': disburse, 'REPAID': repaid, 'ROLLOVER': rollover,
            'UNDRAWN': undrawn, 'PRMTHFIU': prmthfiu,
        })

        if fiu == 0.0:
            continue
        out_rows.append(row)

    if not out_rows:
        return pl.DataFrame()

    pbif = pl.from_dicts(out_rows)

    lday = make_lday(reptdate.year)
    out_rows = []
    for row in pbif.to_dicts():
        inlimit = float(row.get('INLIMIT') or 0.0)
        freq    = 6 if inlimit >= 1000000.0 else 12
        row['FREQ'] = freq

        matdte = date(reptdate.year, reptdate.month, reptdate.day)
        stdates_raw = row.get('STDATES')
        stdates     = sas_date_to_pydate(stdates_raw) if stdates_raw is not None else None

        if stdates is not None and stdates_raw > 0:
            matdte = stdates
            while matdte <= reptdate:
                matdte = nxtbldt(matdte, freq, lday)

        row['MATDTE'] = pydate_to_sasdate(matdte)
        row.pop('CUSTCD', None)
        out_rows.append(row)

    if not out_rows:
        return pl.DataFrame()

    pbif = pl.from_dicts(out_rows)
    pbif = (pbif.sort(['CLIENTNO', 'MATDTE'])
                .unique(subset=['CLIENTNO', 'MATDTE'], keep='first'))
    pbif = pbif.rename({c: c.lower() for c in pbif.columns})
    return pbif


# =============================================================================
# OUTPUT
# =============================================================================

def write_pbif_via_saspy(df: pl.DataFrame, out_path: str,
                         sas_cfgname: str = 'mysas') -> str:
    if saspy is None:
        raise RuntimeError("saspy not installed")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    pdf = df.to_pandas()
    for col in ['stdates', 'matdte', 'STDATES', 'MATDTE']:
        if col in pdf.columns:
            pdf[col] = pdf[col].apply(
                lambda v: sas_date_to_pydate(v) if v is not None else None)
    sas = saspy.SASsession(cfgname=sas_cfgname)
    sas.df2sd(pdf, table='PBIF_OUT', libref='WORK')
    out_dir  = os.path.dirname(out_path).replace('\\', '/')
    out_file = os.path.basename(out_path).replace('.sas7bdat', '')
    sas.submit(f'''
    libname pbifout "{out_dir}";
    data pbifout.{out_file};
        set WORK.PBIF_OUT;
    run;
    ''')
    sas.endsas()
    return out_path


def write_pbif_via_pyreadstat(df: pl.DataFrame, out_path: str) -> str:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    pdf = df.to_pandas()
    for col in ['stdates', 'matdte', 'STDATES', 'MATDTE']:
        if col in pdf.columns:
            pdf[col] = pdf[col].apply(
                lambda v: sas_date_to_pydate(v) if v is not None else None)
    pyreadstat.write_sas7bdat(pdf, out_path)
    return out_path


def main():
    reptdate = get_report_date()
    pbif = build_pbif(reptdate=reptdate)
    if pbif.is_empty():
        print("RDL2PBIF: no output rows produced.")
        return
    out_path = os.path.join(PBIF_OUTPUT_DIR, PBIF_OUTPUT_NAME)
    written = (write_pbif_via_saspy(pbif, out_path) if saspy is not None
               else write_pbif_via_pyreadstat(pbif, out_path))
    print(f"RDL2PBIF: wrote {pbif.height} rows to {written}")


if __name__ == "__main__":
    main()
