#!/usr/bin/env python3
"""
Program Name: RDL2PBIF.py
Purpose:      Process PBIF (Public Bank Invoice Financing) factoring loan data.
              - Loads PBIF client data filtered for entity='PBBH'
              - Merges with MECHRG (mechanism charge) fixed-width text file
              - Computes FIU balance, DISBURSE, REPAID, UNDRAWN
              - Derives MATDTE (next billing date) via NXTBLDT logic
              - Outputs deduplicated PBIF dataset (by CLIENTNO MATDTE)

Changes applied
---------------
- Input read via pyreadstat.read_sas7bdat (replaces duckdb/parquet).
- REPTDATE dataset dependency removed; report date = today - 1 day.
- Output written as .sas7bdat (via saspy, with pyreadstat fallback).

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
import tempfile
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
# REPORT DATE  (replaces the REPTDATE dataset read)
# =============================================================================

def get_report_date() -> date:
    """
    Report date = yesterday (today - 1 day).
    Replaces the SAS 'SET REPTDATE;' one-row lookup dataset.
    """
    return (datetime.today() - timedelta(days=1)).date()


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
#
# SAS:
#   RETAIN D1-D12 31 D4 D6 D9 D11 30
#          RD1-RD12 MD1-MD12 31 RD2 MD2 28 ...
#   ARRAY LDAY D1-D12;
#
# Defaults: all months=31, then Apr/Jun/Sep/Nov overridden to 30, Feb=28.
# Leap-year check uses MOD(YY,4)=0 (SAS simple 4-year rule).
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

    # Normalise column names to upper case to match SAS variable names
    df.columns = [c.upper() for c in df.columns]

    # Filter: IF ENTITY='PBBH'
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
      50-99 → 1950-1999
      00-49 → 2000-2049
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

    &MDATE = the SAS date integer of the report date.
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
      1. Report date = today - 1 (REPTDATE dataset removed)
      2. Load PBIF.CLIEN<YYYY><MM><DD> via pyreadstat, filter ENTITY='PBBH'
      3. Assign fixed fields, reclassify CUSTFISS
      4. Load & summarise MECHRG, merge into PBIF
      5. Compute FIU, BALANCE, DISBURSE, REPAID, UNDRAWN
      6. Compute MATDTE via %NXTBLDT loop
      7. PROC SORT NODUPKEY BY CLIENTNO MATDTE
    Returns the final PBIF DataFrame.
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

    return pbif


# =============================================================================
# OUTPUT — write .sas7bdat via saspy
# =============================================================================

def write_pbif_via_saspy(
    df: pl.DataFrame,
    out_path: str,
    sas_cfgname: str = 'mysas',
) -> str:
    """
    Write the final PBIF DataFrame to a .sas7bdat file using saspy.

    Strategy:
      1. Materialise the polars DataFrame to a pandas DataFrame.
      2. Convert SAS-date integer columns back to real dates so saspy
         writes them as proper SAS date values.
      3. Use SASsession.df2sd to push the DataFrame into a SAS WORK table,
         then use a SAS DATA step to write it out to the .sas7bdat path.

    Returns the output path written.
    """
    if saspy is None:
        raise RuntimeError(
            "saspy is not installed; cannot write .sas7bdat via saspy."
        )

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    # -- Materialise to pandas ------------------------------------------------
    pdf = df.to_pandas()

    # -- Convert SAS-date integer columns to real dates -----------------------
    # These are the columns the SAS program treats as SAS date values.
    sas_date_cols = ['STDATES', 'MATDTE']
    for col in sas_date_cols:
        if col in pdf.columns:
            pdf[col] = pdf[col].apply(
                lambda v: sas_date_to_pydate(v) if v is not None else None
            )

    # -- Connect to SAS and push ---------------------------------------------
    sas = saspy.SASsession(cfgname=sas_cfgname)

    # df2sd pushes the DataFrame into a SAS WORK table named by `table`
    work_tbl = 'PBIF_OUT'
    sas.df2sd(pdf, table=work_tbl, libref='WORK')

    # Now write the WORK table out to .sas7bdat at out_path via a DATA step.
    # SAS needs the directory to exist and the path uses forward slashes.
    out_dir  = os.path.dirname(out_path).replace('\\', '/')
    out_file = os.path.basename(out_path)

    sas_code = f"""
    libname pbifout "{out_dir}";
    data pbifout.{out_file.replace('.sas7bdat', '')};
        set WORK.{work_tbl};
    run;
    """
    sas.submit(sas_code)

    sas.endsas()
    return out_path


def write_pbif_via_pyreadstat(df: pl.DataFrame, out_path: str) -> str:
    """
    Fallback writer: use pyreadstat.write_sas7bdat directly.
    Used when saspy is unavailable or when running outside a SAS environment.
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    pdf = df.to_pandas()

    # Convert SAS-date integer columns to real dates so pyreadstat writes
    # them as proper SAS date values.
    for col in ['STDATES', 'MATDTE']:
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
    # Report date = yesterday (REPTDATE dataset removed)
    reptdate = get_report_date()

    pbif = build_pbif(reptdate=reptdate)

    if pbif.is_empty():
        print("RDL2PBIF: no output rows produced.")
        return

    out_path = os.path.join(PBIF_OUTPUT_DIR, PBIF_OUTPUT_NAME)

    # Prefer saspy; fall back to pyreadstat if saspy is not available.
    if saspy is not None:
        written = write_pbif_via_saspy(pbif, out_path)
    else:
        written = write_pbif_via_pyreadstat(pbif, out_path)

    print(f"RDL2PBIF: wrote {pbif.height} rows to {written}")


if __name__ == "__main__":
    main()
