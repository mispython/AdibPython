# !/usr/bin/env python3
"""
Program Name : NPGS5RPT
Purpose      : Public Bank Berhad - NPGS5 Report Template
               Reusable PROC REPORT equivalent called via %INC PGM(NPGS5RPT)
               from its calling driver program for each schedule code.
               Generates ASA carriage-control detail listing report for
               NPGS5 submissions.

Original SAS:
  PROC   REPORT DATA=NPGS5 NOWD HEADSKIP HEADLINE SPLIT='*';
  COLUMN CVAR02 CVAR03 CVAR04 CVARX1 CVAR06 CVARX5 CVAR16 CVAR08
         CVAR09 CVAR17 ACCRUALX CVAR10 CVAR11 CVAR12A CVAR13
         CVARX2 CVARX3 CVAR01;
  DEFINE CVAR02   / DISPLAY FORMAT=$3.       'SCH';
  DEFINE CVAR03   / DISPLAY FORMAT=$15.      'IC /BUSS. NUM.';
  DEFINE CVAR04   / DISPLAY FORMAT=$100.     'NAME OF CUSTOMER';
  DEFINE CVARX1   / DISPLAY FORMAT=$10.      '              ';
  DEFINE CVAR06   / DISPLAY FORMAT=10.       'ACCOUNT NUMBER';
  DEFINE CVARX5   / DISPLAY FORMAT=$10.      'DISBURSE*DATE';
  DEFINE CVAR16   / DISPLAY FORMAT=$8.       'FACILITY*TYPE';
  DEFINE CVAR08   / DISPLAY FORMAT=10.2      'LOAN AMOUNT';
  DEFINE CVAR09   / DISPLAY FORMAT=10.2      'O/S BALANCE';
  DEFINE CVAR17   / DISPLAY FORMAT=10.2      'PRINCIPAL*BALANCE';
  DEFINE ACCRUALX / DISPLAY FORMAT=10.2      'INTEREST*BALANCE';
  DEFINE CVAR10   / DISPLAY FORMAT=10.2      'CREDIT*BALANCE';
  DEFINE CVAR11   / DISPLAY FORMAT=7.        'ARREARS';
  DEFINE CVAR12A  / DISPLAY FORMAT=$3.       'ST ';
  DEFINE CVAR13   / DISPLAY FORMAT=$10.      'NPL DATE';
  DEFINE CVARX2   / DISPLAY FORMAT=$10.      'NPL*NOTIFICATN*DATE';
  DEFINE CVARX3   / DISPLAY FORMAT=$6.       'NPL*REASON';
  DEFINE CVAR01   / DISPLAY FORMAT=10.       'APPLICATN*NUMBER';
  *;
"""

import os

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
#
# NOTE vs NPGS3RPT/NPGS4RPT:
#   - No TRANCHE column
#   - Adds CVARX5 (DISBURSE DATE — note: SAS format is $10. character, NOT
#     DDMMYY10., so it is treated as a plain pre-formatted string, not
#     converted from a SAS date value)
#   - Adds CVAR16 (FACILITY TYPE), CVAR17 (renamed PRINCIPAL BALANCE, was
#     CURBAL in NPGS3/4), ACCRUALX (renamed INTEREST BALANCE, was ACCRUAL),
#     CVAR10 (CREDIT BALANCE — not present in NPGS3/4)
#   - CVARX3 (NPL REASON) is FORMAT=$6. here, vs $4. in NPGS3RPT/NPGS4RPT
# =============================================================================

REPORT_COLS: list[tuple[str, str, int, str]] = [
    ('cvar02',   'SCH',              3, 'left'),
    ('cvar03',   'IC /BUSS. NUM.',  15, 'left'),
    ('cvar04',   'NAME OF CUSTOMER', 100, 'left'),
    ('cvarx1',   '              ',  10, 'left'),
    ('cvar06',   'ACCOUNT NUMBER',  10, 'right'),
    ('cvarx5',   'DISBURSE*DATE',   10, 'left'),
    ('cvar16',   'FACILITY*TYPE',    8, 'left'),
    ('cvar08',   'LOAN AMOUNT',     10, 'right'),
    ('cvar09',   'O/S BALANCE',     10, 'right'),
    ('cvar17',   'PRINCIPAL*BALANCE', 10, 'right'),
    ('accrualx', 'INTEREST*BALANCE', 10, 'right'),
    ('cvar10',   'CREDIT*BALANCE',  10, 'right'),
    ('cvar11',   'ARREARS',          7, 'right'),
    ('cvar12a',  'ST ',              3, 'left'),
    ('cvar13',   'NPL DATE',        10, 'left'),
    ('cvarx2',   'NPL*NOTIFICATN*DATE', 10, 'left'),
    ('cvarx3',   'NPL*REASON',       6, 'left'),
    ('cvar01',   'APPLICATN*NUMBER', 10, 'right'),
]

# Columns rendered via numeric FORMAT=n. or FORMAT=n.d in the SAS DEFINE
_NUMERIC_2DP = {'cvar08', 'cvar09', 'cvar17', 'accrualx', 'cvar10'}  # FORMAT=10.2
_NUMERIC_0DP = {'cvar06', 'cvar11', 'cvar01'}                        # FORMAT=10. / 7.

# Total width of one report body line
_TOTAL_WIDTH: int = (
    sum(w for _, _, w, _ in REPORT_COLS)
    + len(COL_SEP) * (len(REPORT_COLS) - 1)
)

# =============================================================================
# INTERNAL HELPERS
# =============================================================================

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
    if col_name in _NUMERIC_2DP:
        s = _fmt_numeric(val, width, 2)
    elif col_name in _NUMERIC_0DP:
        s = _fmt_numeric(val, width, 0)
    else:
        # FORMAT=$n.  — character, left-pad/truncate to width
        # (includes cvarx5 'DISBURSE DATE', which is $10. character in the
        # original SAS — NOT converted from a SAS numeric date value)
        s = _coalesce_s(val)[:width]

    return s.rjust(width) if align == 'right' else s.ljust(width)

# =============================================================================
# PUBLIC INTERFACE
# =============================================================================

def npgs5_report(
    df:          pl.DataFrame,
    report_path: str,
    title1:      str,
    title2:      str,
) -> None:
    """
    Generate an ASA carriage-control NPGS5 detail listing report.

    Equivalent to the SAS block:
        PROC PRINTTO PRINT=<output>;
        TITLE1 '<title1>';
        TITLE2 '<title2>';
        %INC PGM(NPGS5RPT);

    ASA carriage-control characters (first byte of each line):
        '1'  — page eject (new page)
        ' '  — single space (normal print)

    Parameters
    ----------
    df          : Polars DataFrame — already filtered / derived as NPGS5,
                  and sorted as required by the caller.
    report_path : Destination file path for the ASA report.
    title1      : TITLE1 text  (e.g. 'PUBLIC BANK BERHAD')
    title2      : TITLE2 text  (e.g. 'NPGS5 DETAIL OF ACCTS ...')
    """
    headline     = '-' * _TOTAL_WIDTH
    header_lines = _build_header_lines()

    _page_overhead = 3 + len(header_lines) + 1

    output_lines: list[str] = []
    line_cnt:     int       = PAGE_LENGTH + 1   # force first page immediately

    def _new_page() -> None:
        nonlocal line_cnt
        output_lines.append('1' + title1)       # '1' = ASA page eject
        output_lines.append(' ' + title2)       # ' ' = ASA normal single space
        output_lines.append(' ')                # HEADSKIP — one blank line
        for hdr in header_lines:
            output_lines.append(' ' + hdr)
        output_lines.append(' ' + headline)     # HEADLINE — underline rule
        line_cnt = _page_overhead

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

    out_dir = os.path.dirname(report_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(report_path, 'w', encoding='utf-8', newline='\n') as fh:
        for ln in output_lines:
            fh.write(ln + '\n')
