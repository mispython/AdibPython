PTIONS YEARCUTOFF=1940 SORTDEV=3390 NONUMBER NODATE NOCENTER;
*;
DATA REPTDATE (KEEP=REPTDATE);
  SET MNILN.REPTDATE;
  MM =MONTH(REPTDATE);
  MM1=MM - 1;
  IF MM1 = 0 THEN MM1 = 12;
  CALL SYMPUT('REPTMON',PUT(MM,Z2.));
  CALL SYMPUT('REPTMON1',PUT(MM1,Z2.));
  CALL SYMPUT('REPTYEAR',PUT(REPTDATE,YEAR4.));
  CALL SYMPUT('REPTDAY',PUT(DAY(REPTDATE),Z2.));
  CALL SYMPUT('RDATE',PUT(REPTDATE,DDMMYY8.));
  CALL SYMPUT('NDATE',PUT(REPTDATE,Z5.));
RUN;
*;
 /********** SC53 **********/
DATA SC53;
   SET NPGS.BTNPGS&REPTMON
       NPGS.LNNPGS&REPTMON
       NPGS.DPNPGS&REPTMON;
   IF  CVAR02='53';
   IF  NATGUAR='06' AND CINSTCL='18';
   CVARXX='          ';
   IF  CVAR11 < 3  THEN CVAR12='   ';
   CVAR02='E1';
RUN;
PROC SORT; BY CVAR01 CVAR06; RUN;

 /********** SCEI **********/
DATA SCEI;
   SET NPGSI.DPNPGS&REPTMON
       NPGSI.LNIPGS&REPTMON;
   IF CVAR12='NPL'  THEN CVAR12='NP';
   ELSE                  CVAR12='AP';
   IF  NATGUAR='06' AND CINSTCL='18';
   CVARXX='          ';
   CVAR02='E2';
RUN;
PROC SORT; BY CVAR01 CVAR06; RUN;

 /********** OTH **********/
DATA OTH;
   SET NPGS.LNNPGS&REPTMON;
   IF  CVAR02 IN ('81','2Z','4Z','H4','H5','H6','H7',
                  'F5','F6','1Z','3Z','5S','6S','1H',
                  '2H','3H','4H','E6','5Z','5H','6H');

   IF  NATGUAR='06' AND CINSTCL='18';
   CVARXX='          ';

   IF CVAR02 = '81' THEN DO;
      IF  CVAR11 < 3  THEN CVAR12='   ';
      CVAR02='G1';
   END;
   ELSE IF CVAR02 IN ('2Z','4Z','F6') THEN DO;
      CVAR07 = 'TF';
      IF  CVAR11 < 3   THEN CVAR12='   ';
      ELSE                  CVAR12= 'NPF';
   END;
   ELSE IF CVAR02 = 'H4' THEN DO;
      CVAR07 = 'TL';
      IF  CVAR11 < 3   THEN CVAR12='   ';
      IF  CVAR12 = ' ' THEN CVAR12='AP';
   END;
   ELSE IF CVAR02 = 'H4' THEN DO;
      CVAR07 = 'TF';
      IF  CVAR11 < 3   THEN CVAR12='   ';
      IF  CVAR12 = ' ' THEN CVAR12='AP';
   END;
   ELSE IF CVAR02 = 'H6' THEN DO;
      CVAR07 = 'FL';
      IF  CVAR11 < 3   THEN CVAR12='   ';
      IF CVAR12='   '  THEN CVAR13='          ';
   END;
   ELSE IF CVAR02 = 'H7' THEN DO;
      CVAR07 = 'TF';
      IF  CVAR11 < 3   THEN CVAR12='   ';
      IF  CVAR12='   ' THEN CVAR13='          ';
   END;
   ELSE IF CVAR02 = 'F5' THEN DO;
      CVAR07 = 'FL';
      IF  CVAR11 < 3   THEN CVAR12='   ';
   END;
   ELSE IF CVAR02 IN ('1Z','3Z','5S','1H','3H') THEN DO;
      CVAR07 = 'FL';
      IF  CVAR11 < 3   THEN CVAR12='   ';
      ELSE       CVAR12= 'NPL';
   END;
   ELSE IF CVAR02 = '6S' THEN DO;
      CVAR07 = 'TF';
      IF  CVAR11 < 3   THEN CVAR12='   ';
      ELSE       CVAR12= 'NPL';
   END;
   ELSE IF CVAR02 IN ('2H','4H') THEN DO;
      CVAR07 = 'TL';
      IF  CVAR11 < 3   THEN CVAR12='   ';
      ELSE       CVAR12= 'NPF';
   END;
   ELSE IF CVAR02 IN ('E6','5Z','5H','6H') THEN DO;
      IF CVAR11 < 3    THEN CVAR12='   ';
      IF CVAR12='   '  THEN CVAR13='          ';
   END;
RUN;
PROC SORT DATA=OTH; BY CVAR01 CVAR06; RUN;

DATA NPGS;
   SET SC53 SCEI OTH;
RUN;
PROC SORT DATA=NPGS; BY CVAR02 CVAR01 CVAR06; RUN;

DATA COMB;
   SET NPGS;
   LASTCOL = '';
   FORMAT CVAR05 DDMMYY10. CVAR08 CVAR09 CVAR10 20.2;
   FILE COMBT DSD DLM=';';
   PUT  @1 CVAR01
           CVAR02
           CVAR03
           CVAR04
           CVAR05
           CVAR06
           CVAR07
           CVAR08
           CVAR09
           CVAR10
           CVAR11
           CVAR12
           CVAR13
           CVAR14
           CVAR15
           LASTCOL
           ;
RUN;

PROC    PRINTTO PRINT=COMBR;
TITLE1 'PUBLIC BANK BERHAD';
TITLE2 'DETAIL OF ACCTS NON-PG FOR SUBMISSION TO CGC @' &RDATE;
%INC PGM(NPGSRPT);


this is the sas orignal code, include the NPGSRPT.py as per SAS code above



below is the NPGSRPT.py:

# !/usr/bin/env python3
"""
Program Name : NPGSRPT
Purpose      : Public Bank Berhad - NPGS CGC Report Template
               Reusable PROC REPORT equivalent called via %INC PGM(NPGSRPT) from EIBRNPGS for each schedule code.
               Generates ASA carriage-control detail listing report for
                NPGS (National Programmes Guarantee Scheme) CGC submissions.

Original SAS:
  PROC REPORT DATA=NPGS NOWD HEADSKIP HEADLINE SPLIT='*';
  COLUMN CVAR01 CVAR02 CVAR03 CVAR04 CVAR05 CVAR06 CVAR07 CVAR08
         CVARXX CVAR09 CVAR10 CVAR11 CVAR12 CVAR13 CVAR14 CVAR15 BRANCH;
  DEFINE CVAR01  / DISPLAY FORMAT=10.        'REFER.NUM ';
  DEFINE CVAR02  / DISPLAY FORMAT=$3.        'SCH';
  DEFINE CVAR03  / DISPLAY FORMAT=$15.       'IC /BUSS. NUM.';
  DEFINE CVAR04  / DISPLAY FORMAT=$50.       'NAME OF CUSTOMER';
  DEFINE CVAR05  / DISPLAY FORMAT=DDMMYY10.  'DISBURSE';
  DEFINE CVARXX  / DISPLAY FORMAT=$10.       '              ';
  DEFINE CVAR06  / DISPLAY FORMAT=10.        'ACCOUNT NUMBER';
  DEFINE CVAR07  / DISPLAY FORMAT=$2.        'TY';
  DEFINE CVAR08  / DISPLAY FORMAT=13.2       'APPROVE LIMIT';
  DEFINE CVAR09  / DISPLAY FORMAT=13.2       'DEBIT  BALANCE';
  DEFINE CVAR10  / DISPLAY FORMAT=13.2       'CREDIT BALANCE';
  DEFINE CVAR11  / DISPLAY FORMAT=7.         'ARREARS';
  DEFINE CVAR12  / DISPLAY FORMAT=$3.        'ST ';
  DEFINE CVAR13  / DISPLAY FORMAT=$10.       'NPL DATE';
  DEFINE CVAR14  / DISPLAY FORMAT=$4.        'FI  CODE';
  DEFINE CVAR15  / DISPLAY FORMAT=$5.        'MICR CODE';
  DEFINE BRANCH  / DISPLAY FORMAT=3.         'BRH';
  *;
"""

import os
from datetime import date, timedelta
from typing import Optional

import polars as pl

# =============================================================================
# CONSTANTS
# =============================================================================

PAGE_LENGTH = 60    # lines per page
COL_SEP     = ' '   # single space between columns (PROC REPORT default)

# =============================================================================
# COLUMN DEFINITIONS
# (col_name, header_label, display_width, alignment)
# SPLIT='*' in label means '*' creates a line break within the header cell.
# =============================================================================

REPORT_COLS: list[tuple[str, str, int, str]] = [
    ('cvar01', 'REFER.NUM ',      10, 'right'),
    ('cvar02', 'SCH',              3, 'left'),
    ('cvar03', 'IC /BUSS. NUM.',  15, 'left'),
    ('cvar04', 'NAME OF CUSTOMER', 50, 'left'),
    ('cvar05', 'DISBURSE',        10, 'left'),
    ('cvarxx', '              ',  10, 'left'),
    ('cvar06', 'ACCOUNT NUMBER',  10, 'right'),
    ('cvar07', 'TY',               2, 'left'),
    ('cvar08', 'APPROVE LIMIT',   13, 'right'),
    ('cvar09', 'DEBIT  BALANCE',  13, 'right'),
    ('cvar10', 'CREDIT BALANCE',  13, 'right'),
    ('cvar11', 'ARREARS',          7, 'right'),
    ('cvar12', 'ST ',              3, 'left'),
    ('cvar13', 'NPL DATE',        10, 'left'),
    ('cvar14', 'FI  CODE',         4, 'left'),
    ('cvar15', 'MICR CODE',        5, 'left'),
    ('branch', 'BRH',              3, 'right'),
]

# Total width of one report body line
_TOTAL_WIDTH: int = (
    sum(w for _, _, w, _ in REPORT_COLS)
    + len(COL_SEP) * (len(REPORT_COLS) - 1)
)

# =============================================================================
# INTERNAL HELPERS
# =============================================================================

def _sas_date_to_pydate(val) -> Optional[date]:
    """Convert SAS date integer (days since 1960-01-01) to Python date."""
    if val is None or (isinstance(val, float) and val != val):
        return None
    if isinstance(val, (int, float)):
        return date(1960, 1, 1) + timedelta(days=int(val))
    if isinstance(val, date):
        return val
    return None


def _fmt_ddmmyy10(val) -> str:
    """Format date value as DD/MM/YYYY (SAS DDMMYY10. format)."""
    if val is None:
        return '          '
    if isinstance(val, (int, float)):
        val = _sas_date_to_pydate(val)
    if val is None:
        return '          '
    return val.strftime('%d/%m/%Y')


def _fmt_numeric(val, width: int, decimals: int) -> str:
    """
    Right-justify numeric value to <width> characters with <decimals> places.
    Missing/NaN values render as spaces (SAS missing value behaviour).
    """
    if val is None or (isinstance(val, float) and val != val):
        return ' ' * width
    v = float(val)
    s = f"{v:{width}.{decimals}f}" if decimals > 0 else f"{int(round(v)):{width}d}"
    # Truncate from left if overflow (SAS renders asterisks; preserve rightmost digits)
    return s[-width:] if len(s) > width else s


def _coalesce_s(val, default: str = '') -> str:
    """Return stripped string or default when None."""
    return str(val).strip() if val is not None else default


def _build_header_lines() -> list[str]:
    """
    Build column header rows, respecting SPLIT='*' multi-line header labels.
    Each '*' in a label splits it across additional header lines (top-aligned).
    Returns a list of fully formatted header line strings (no ASA prefix).
    """
    split_labels = [label.split('*') for _, label, _, _ in REPORT_COLS]
    max_lines    = max(len(parts) for parts in split_labels)

    # Pad every column to max_lines lines — prepend blanks (top-align)
    padded: list[tuple[list[str], int, str]] = []
    for (_, _, width, align), parts in zip(REPORT_COLS, split_labels):
        while len(parts) < max_lines:
            parts.insert(0, '')
        padded.append((parts, width, align))

    header_rows: list[str] = []
    for line_idx in range(max_lines):
        cells = []
        for parts, width, align in padded:
            raw  = parts[line_idx][:width]
            cell = raw.ljust(width) if align == 'left' else raw.rjust(width)
            cells.append(cell)
        header_rows.append(COL_SEP.join(cells))

    return header_rows


def _format_cell(col_name: str, val, width: int, align: str) -> str:
    """Format one data cell according to its DEFINE specification."""
    if col_name == 'cvar05':
        # FORMAT=DDMMYY10.
        s = _fmt_ddmmyy10(val)
    elif col_name in ('cvar08', 'cvar09', 'cvar10'):
        # FORMAT=13.2
        s = _fmt_numeric(val, 13, 2)
    elif col_name == 'cvar11':
        # FORMAT=7.
        s = _fmt_numeric(val, 7, 0)
    elif col_name in ('cvar01', 'cvar06'):
        # FORMAT=10.
        s = _fmt_numeric(val, width, 0)
    elif col_name == 'branch':
        # FORMAT=3.
        s = _fmt_numeric(val, 3, 0)
    else:
        # FORMAT=$n.  — character, left-pad/truncate to width
        s = _coalesce_s(val)[:width]

    return s.rjust(width) if align == 'right' else s.ljust(width)

# =============================================================================
# PUBLIC INTERFACE
# =============================================================================

def npgs_report(
    df:          pl.DataFrame,
    report_path: str,
    title1:      str,
    title2:      str,
) -> None:
    """
    Generate an ASA carriage-control NPGS CGC detail listing report.

    Equivalent to the SAS block:
        PROC PRINTTO PRINT=<output>;
        TITLE1 '<title1>';
        TITLE2 '<title2>';
        %INC PGM(NPGSRPT);

    ASA carriage-control characters (first byte of each line):
        '1'  — page eject (new page)
        ' '  — single space (normal print)

    Parameters
    ----------
    df          : Polars DataFrame — already filtered, CVAR07 overridden,
                  and sorted BY CVAR01 CVAR06 by the caller (EIBRNPGS).
    report_path : Destination file path for the ASA report.
    title1      : TITLE1 text  (e.g. 'PUBLIC BANK BERHAD')
    title2      : TITLE2 text  (e.g. 'DETAIL OF ACCTS (SCH=70) ...')
    """
    headline     = '-' * _TOTAL_WIDTH
    header_lines = _build_header_lines()

    # Number of fixed overhead lines written per new page:
    #   TITLE1 + TITLE2 + HEADSKIP blank + header row(s) + HEADLINE rule
    _page_overhead = 3 + len(header_lines) + 1

    output_lines: list[str] = []
    line_cnt:     int       = PAGE_LENGTH + 1   # force first page immediately

    def _new_page() -> None:
        nonlocal line_cnt
        # '1' = ASA page eject
        output_lines.append('1' + title1)
        # ' ' = ASA normal single space
        output_lines.append(' ' + title2)
        output_lines.append(' ')                       # HEADSKIP — one blank line
        for hdr in header_lines:
            output_lines.append(' ' + hdr)
        output_lines.append(' ' + headline)            # HEADLINE — underline rule
        line_cnt = _page_overhead

    # Always open with at least one page (handles empty input gracefully)
    _new_page()

    if not df.is_empty():
        for row in df.iter_rows(named=True):
            if line_cnt >= PAGE_LENGTH:
                _new_page()

            cells = [
                _format_cell(col_name, row.get(col_name), width, align)
                for col_name, _, width, align in REPORT_COLS
            ]
            output_lines.append(' ' + COL_SEP.join(cells))
            line_cnt += 1

    # Ensure output directory exists
    out_dir = os.path.dirname(report_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(report_path, 'w', encoding='utf-8', newline='\n') as fh:
        for ln in output_lines:
            fh.write(ln + '\n')


