#!/usr/bin/env python3
"""
Program Name: RDL2PBIF.py
Purpose:      Process PBIF (Public Bank Invoice Financing) factoring loan data.
              - Loads PBIF client data filtered for entity='PBBH'
              - Merges with MECHRG (mechanism charge) fixed-width text file
              - Computes FIU balance, DISBURSE, REPAID, UNDRAWN
              - Derives MATDTE (next billing date) via NXTBLDT logic
              - Outputs deduplicated PBIF dataset (by CLIENTNO MATDTE)
              - Provides format_fisstype() and format_fissgroup() which are
                the $FISSTYPE / $FISSGROUP sector classification helpers used
                by EIMBNM01 for the sector/sub-sector tabulations.

Changes applied
---------------
- Input read via pyreadstat.read_sas7bdat (replaces duckdb/parquet).
- REPTDATE dataset dependency removed; report date = last day of previous month
  (aligned with EIMBNM01 so the two programs agree on the reporting period).
- Output written as .sas7bdat (via saspy, with pyreadstat fallback).
- Output columns lowercased so callers (EIMBNM01) can consume them uniformly.

Dependency notes
----------------
%INC PGM(PBBLNFMT) is present in the SAS source as a session-level include.
Scanning every PUT(x, fmt.) call in the SAS body reveals that no PBBLNFMT
format function (LNPROD, LNDENOM, LNRATE, etc.) is called anywhere in this
program's DATA steps.  All assignments are literal strings or arithmetic.
No import from PBBLNFMT is therefore required or added here.

  PBBLNFMT : session-level include – no format called in this program
"""

import os
from datetime import date, datetime, timedelta
from typing import Optional

import pyreadstat
import polars as pl

# saspy is optional at import time so the module can still be linted/tested
try:
    import saspy
except ImportError:  # pragma: no cover
    saspy = None


# =============================================================================
# PATH CONFIGURATION
# =============================================================================

PBIF_CLIEN_DIR    = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/RDL2PBIF"
MECHRG_TXT        = f"{PBIF_CLIEN_DIR}/mechrg.txt"
PBIF_OUTPUT_DIR   = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/prod/RDL2PBIF"
PBIF_OUTPUT_NAME  = "pbif_output.sas7bdat"


# =============================================================================
# REPORT DATE  (aligned with EIMBNM01)
# =============================================================================

def get_report_date() -> date:
    """
    Report date = last day of previous month.
    Matches EIMBNM01.get_report_vars()['reptdate'].
    Replaces the SAS 'SET REPTDATE;' one-row lookup dataset.
    """
    today = date.today()
    return today.replace(day=1) - timedelta(days=1)


def report_date_parts(d: date) -> dict:
    """Return the string parts used to build the clien<YY><MM><DD> filename."""
    return {
        "reptyear": f"{d.year:04d}",
        "reptmon":  f"{d.month:02d}",
        "reptday":  f"{d.day:02d}",
    }


# =============================================================================
# DATE HELPERS
# =============================================================================

def sas_date_to_pydate(val) -> Optional[date]:
    """Convert SAS date integer (days since 1960-01-01) to Python date."""
    if val is None or (isinstance(val, float) and val != val):
        return None
    if isinstance(val, (int, float)):
        return date(1960, 1, 1) + timedelta(days=int(val))
    if isinstance(val, date):
        return val
    return None


def pydate_to_sasdate(d: date) -> int:
    """Convert Python date to SAS date integer (days since 1960-01-01)."""
    return (d - date(1960, 1, 1)).days


# =============================================================================
# %MACRO DCLVAR — day arrays
# =============================================================================

def make_lday(year: int) -> list:
    """
    Build LDAY array (index 1..12) matching SAS %MACRO DCLVAR defaults,
    using the SAS simple leap-year rule: MOD(YY,4)=0.
    """
    lday = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    lday[2] = 29 if (year % 4 == 0) else 28
    return lday


# =============================================================================
# %MACRO NXTBLDT — compute next billing date
# =============================================================================

def nxtbldt(matdte: date, freq: int, lday: list) -> date:
    """
    Advance MATDTE by FREQ months, clamping DD to month-end when needed.
    Uses the SAS simple leap-year rule (year % 4 == 0) for Feb.
    """
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
# SAS FORMAT PORTS — $FISSTYPE / $FISSGROUP
#
# These functions reproduce the SAS PROC FORMAT entries that EIMBNM01 uses
# in its PROC TABULATE statements:
#
#     SECTYPE  = PUT(SECTORCD, $FISSTYPE.);
#     SECGROUP = PUT(SECTORCD, $FISSGROUP.);
#
# NOTE
# ----
# The mappings below are PLACEHOLDERS.  Replace them with the real
# $FISSTYPE / $FISSGROUP entries from your SAS PROC FORMAT catalog
# (typically found in a PGM or FORMATS catalog sourced at session start,
# or in the SAS format source .sas file).  Until the real mappings are
# filled in, sector/sub-sector tabulations in EIMBNM01 will be incorrect.
# =============================================================================

# --- $FISSTYPE: SECTORCD -> sub-sector label -------------------------------
# Replace with the actual SAS $FISSTYPE values.
_FISSTYPE_MAP = {
    # '01': 'AGRICULTURE',
    # '02': 'MINING AND QUARRYING',
    # '03': 'MANUFACTURING',
    # ... fill in from PROC FORMAT $FISSTYPE ...
}

# --- $FISSGROUP: SECTORCD -> sector group label ----------------------------
# Replace with the actual SAS $FISSGROUP values.
_FISSGROUP_MAP = {
    # '01': 'PRIMARY',
    # '02': 'SECONDARY',
    # '03': 'TERTIARY',
    # ... fill in from PROC FORMAT $FISSGROUP ...
}


def format_fisstype(sectorcd) -> str:
    """
    Port of SAS format $FISSTYPE.
    Falls back to the raw SECTORCD (zero-padded to the width used by the
    format) when no mapping is present.
    """
    s = str(sectorcd or '').strip()
    if not s:
        return ''
    # SAS $FISSTYPE typically maps 4-char codes like '0470'.
    # If your SECTORCD arrives as an int/float, zero-pad to 4 chars.
    if s.replace('.', '', 1).isdigit():
        try:
            s = f"{int(float(s)):04d}"
        except (ValueError, TypeError):
            pass
    return _FISSTYPE_MAP.get(s, s)


def format_fissgroup(sectorcd) -> str:
    """
    Port of SAS format $FISSGROUP.
    Falls back to the raw SECTORCD when no mapping is present.
    """
    s = str(sectorcd or '').strip()
    if not s:
        return ''
    if s.replace('.', '', 1).isdigit():
        try:
            s = f"{int(float(s)):04d}"
        except (ValueError, TypeError):
            pass
    return _FISSGROUP_MAP.get(s, s)


# =============================================================================
# LOAD PBIF.CLIEN<YYYY><MM><DD>  via pyreadstat  (SAS7BDAT input)
# =============================================================================

def load_clien(clien_path: str) -> pl.DataFrame:
    """
    Read the SAS7BDAT client file with pyreadstat and filter ENTITY='PBBH'.

    pyreadstat returns a pandas DataFrame; convert to polars for the rest
    of the pipeline.  Column names are upper-cased to match SAS.
    """
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
# LOAD MECHRG — fixed-width text file
# =============================================================================

def _parse_informat_12_2(raw: str) -> float:
    """
    Parse a SAS 12.2 numeric informat field.
    If the string contains an explicit decimal point, convert directly.
    If not, apply implied 2 decimal places (divide by 100).
    Returns 0.0 for blank/unparseable fields.
    """
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
    """
    Parse an 8-character YYMMDD8. field using SAS YEARCUTOFF=1950.
      50-99 -> 1950-1999
      00-49 -> 2000-2049
    Returns None if the string is blank or malformed.
    """
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
    """
    Read MECHRG fixed-width text file, filter to PDATE == &MDATE, then
    PROC SUMMARY (SUM INTVAL BY CLIENTNO).
    """
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

                uval1  = _parse_informat_12_2(uval1_str)
                uval2  = _parse_informat_12_2(uval2_str)
                uval3  = _parse_informat_12_2(uval3_str)
                intval = uval1 + uval2 + uval3

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
    """Reclassify CUSTFISS per the SAS IF-ELSE chain."""
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
# MAIN BUILD FUNCTION
# =============================================================================

def build_pbif(reptdate: Optional[date] = None) -> pl.DataFrame:
    """
    Full RDL2PBIF logic:
      1. Report date = last day of previous month (aligned with EIMBNM01)
      2. Load PBIF.CLIEN<YYYY><MM><DD> via pyreadstat, filter ENTITY='PBBH'
      3. Assign fixed fields, reclassify CUSTFISS
      4. Load & summarise MECHRG, merge into PBIF
      5. Compute FIU, BALANCE, DISBURSE, REPAID, UNDRAWN
      6. Compute MATDTE via %NXTBLDT loop
      7. PROC SORT NODUPKEY BY CLIENTNO MATDTE
      8. Return with lowercased column names (for EIMBNM01 consumption)

    NOTE: callers (e.g. EIMBNM01) may pass reptdate; if omitted, the
    module-level get_report_date() is used.
    """
    if reptdate is None:
        reptdate = get_report_date()

    parts      = report_date_parts(reptdate)
    reptyear   = parts["reptyear"]
    reptmon    = parts["reptmon"]
    reptday    = parts["reptday"]
    mdate_int  = pydate_to_sasdate(reptdate)

    # -------------------------------------------------------------------------
    # Step 1 — Load CLIENT file (pyreadstat) and filter ENTITY='PBBH'
    # -------------------------------------------------------------------------
    clien_path = os.path.join(
        PBIF_CLIEN_DIR,
        f"clien{reptyear}{reptmon}{reptday}.sas7bdat",
    )
    pbif = load_clien(clien_path)
    if pbif.is_empty():
        return pl.DataFrame()

    # -------------------------------------------------------------------------
    # Step 2 — Fixed fields + CUSTFISS reclassification
    # -------------------------------------------------------------------------
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

    # -------------------------------------------------------------------------
    # Step 3 — Load MECHRG and merge
    # -------------------------------------------------------------------------
    mechrg_df = load_mechrg(mdate_int)

    if not mechrg_df.is_empty():
        pbif = pbif.join(mechrg_df, on='CLIENTNO', how='left')
    else:
        if 'INTVAL' not in pbif.columns:
            pbif = pbif.with_columns(
                pl.lit(None).cast(pl.Float64).alias('INTVAL')
            )

    # -------------------------------------------------------------------------
    # Step 4 — FIU / BALANCE / DISBURSE / REPAID / UNDRAWN
    # -------------------------------------------------------------------------
    out_rows = []
    for row in pbif.to_dicts():
        fiu      = float(row.get('FIU')      or 0.0)
        prmthfiu = float(row.get('PRMTHFIU') or 0.0)

        if fiu == 0.0 and prmthfiu == 0.0:
            continue

        intval_raw = row.get('INTVAL')
        intval = (
            0.0
            if intval_raw is None or (isinstance(intval_raw, float) and intval_raw != intval_raw)
            else float(intval_raw)
        )
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
                repaid   = prmthfiu - balance

        inlimit = float(row.get('INLIMIT') or 0.0)
        undrawn = inlimit - balance

        row['FIU']      = fiu
        row['BALANCE']  = balance
        row['UFIU']     = ufiu
        row['DISBURSE'] = disburse
        row['REPAID']   = repaid
        row['ROLLOVER'] = rollover
        row['UNDRAWN']  = undrawn
        row['PRMTHFIU'] = prmthfiu

        if fiu == 0.0:
            continue

        out_rows.append(row)

    if not out_rows:
        return pl.DataFrame()

    pbif = pl.from_dicts(out_rows)

    # -------------------------------------------------------------------------
    # Step 5 — MATDTE via %NXTBLDT
    # -------------------------------------------------------------------------
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

    # -------------------------------------------------------------------------
    # Step 6 — Dedup by CLIENTNO MATDTE
    # -------------------------------------------------------------------------
    pbif = (
        pbif
        .sort(['CLIENTNO', 'MATDTE'])
        .unique(subset=['CLIENTNO', 'MATDTE'], keep='first')
    )

    # -------------------------------------------------------------------------
    # Step 7 — Lowercase all column names so EIMBNM01 can consume them.
    # -------------------------------------------------------------------------
    pbif = pbif.rename({c: c.lower() for c in pbif.columns})

    return pbif


# =============================================================================
# OUTPUT — write .sas7bdat via saspy (with pyreadstat fallback)
# =============================================================================

def write_pbif_via_saspy(
    df: pl.DataFrame,
    out_path: str,
    sas_cfgname: str = 'mysas',
) -> str:
    """
    Write the final PBIF DataFrame to a .sas7bdat file using saspy.
    """
    if saspy is None:
        raise RuntimeError(
            "saspy is not installed; cannot write .sas7bdat via saspy."
        )

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    pdf = df.to_pandas()

    # These SAS date columns should be written as proper SAS date values.
    sas_date_cols = ['stdates', 'matdte', 'STDATES', 'MATDTE']
    for col in sas_date_cols:
        if col in pdf.columns:
            pdf[col] = pdf[col].apply(
                lambda v: sas_date_to_pydate(v) if v is not None else None
            )

    sas = saspy.SASsession(cfgname=sas_cfgname)

    work_tbl = 'PBIF_OUT'
    sas.df2sd(pdf, table=work_tbl, libref='WORK')

    out_dir  = os.path.dirname(out_path).replace('\\', '/')
    out_file = os.path.basename(out_path).replace('.sas7bdat', '')

    sas_code = f"""
    libname pbifout "{out_dir}";
    data pbifout.{out_file};
        set WORK.{work_tbl};
    run;
    """
    sas.submit(sas_code)

    sas.endsas()
    return out_path


def write_pbif_via_pyreadstat(df: pl.DataFrame, out_path: str) -> str:
    """
    Fallback writer: use pyreadstat.write_sas7bdat directly.
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    pdf = df.to_pandas()

    for col in ['stdates', 'matdte', 'STDATES', 'MATDTE']:
        if col in pdf.columns:
            pdf[col] = pdf[col].apply(
                lambda v: sas_date_to_pydate(v) if v is not None else None
            )

    pyreadstat.write_sas7bdat(pdf, out_path)
    return out_path


# =============================================================================
# ENTRY POINT
# =============================================================================

def main():
    reptdate = get_report_date()

    pbif = build_pbif(reptdate=reptdate)

    if pbif.is_empty():
        print("RDL2PBIF: no output rows produced.")
        return

    out_path = os.path.join(PBIF_OUTPUT_DIR, PBIF_OUTPUT_NAME)

    if saspy is not None:
        written = write_pbif_via_saspy(pbif, out_path)
    else:
        written = write_pbif_via_pyreadstat(pbif, out_path)

    print(f"RDL2PBIF: wrote {pbif.height} rows to {written}")


if __name__ == "__main__":
    main()
