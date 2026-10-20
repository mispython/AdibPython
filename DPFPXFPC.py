# EIMREPOI_REPO_PROCESSOR.py
"""
EIMREPOI - Repossession Listing Report

Faithful Python port of the SAS job that produces:
  - REPOTXT.txt   (all repossession accounts, ARREAR >= 10)
  - REPOTXT1.txt  (subset: ARREAR >= 10 AND LOANTYPE IN 983, 993)

Inputs:
  - enrh_ln_note_mMM.sas7bdat   (LNNOTE - huge, chunked/col-pruned read)
  - loantemp.sas7bdat           (LOANTEMP - arrears lookup from ARREAR lib)
  - stg_ln_nam8file.sas7bdat    (NAME8 - engine / chassis)

Formats/macros from PBBLNFMT and PBBELF.

SAS reference logic:
  PROC SORT DATA=LOAN.LNNOTE OUT=LNNOTE (KEEP=ACCTNO LOANTYPE NTBRCH COLLDESC COLLYEAR);
       WHERE LOANTYPE IN &HP AND BALANCE GT 0 AND BORSTAT NOT IN ('F','I','R');
  PROC SORT DATA=LOAN.NAME8 OUT=NAME8 (KEEP=ACCTNO LINETHRE LINEFOUR);
  PROC SORT DATA=ARREAR.LOANTEMP OUT=ARREAR (KEEP=ACCTNO ARREAR);
  DATA REPO; MERGE LNNOTE(IN=AA) NAME8(...) ARREAR; BY ACCTNO; IF AA;
       BRABBR = PUT(NTBRCH, BRCHCD.);
       CAC    = PUT(NTBRCH, CACNAME.);
       MAKE   = SUBSTR(COLLDESC, 1, 16);
       MODEL  = SUBSTR(COLLDESC, 16, 21);
       REGNO  = SUBSTR(COLLDESC, 40, 13);
  DATA REPO REPO1; SET REPO; IF ARREAR GE 10; OUTPUT REPO;
       IF LOANTYPE IN (983,993) THEN OUTPUT REPO1;
  PROC SORT DATA=REPO OUT=REPO; BY REGNO;
  PROC SORT DATA=REPO1 OUT=REPO1; BY REGNO;
  (then DATA _NULL_ fixed-width writes to REPOTXT / REPOTXT1)
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pyreadstat

import PBBLNFMT
import PBBELF


# =====================================================================
# Constants
# =====================================================================
HP_VALUES_INT   = list(PBBLNFMT.MACROS['HP'])            # 17 HP loan types
HP_VALUES_STR   = [str(x) for x in HP_VALUES_INT]
REPORT_LOANTYPES = ('983', '993')                        # REPO1 subset
ARREAR_THRESHOLD = 10                                    # IF ARREAR GE 10
REPORT_LINE_WIDTH = 167                                  # @164 $4. ends at 167

# Expected record-count band for sanity checking (adjust as business requires)
EXPECTED_REPO_MIN  = 500
EXPECTED_REPO_MAX  = 5000

INPUT_BASE  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod")
OUTPUT_BASE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMREPOI")


# =====================================================================
# Helpers
# =====================================================================
def normalize_code(series: pd.Series) -> pd.Series:
    """Normalize numeric/string codes to clean int-string form."""
    def _clean(x):
        if pd.isna(x):
            return None
        if isinstance(x, bytes):
            x = x.decode('utf-8', errors='ignore')
        try:
            return str(int(float(x)))
        except (ValueError, TypeError):
            return str(x).strip()
    return series.apply(_clean)


def normalize_acctno(series: pd.Series) -> pd.Series:
    """Normalize ACCTNO for merging (float 2900189623.0 -> '2900189623')."""
    return normalize_code(series)


def to_int_code(val):
    """Convert a scalar to int for format lookups. Returns None on failure."""
    if pd.isna(val):
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def put_field(buf: list, start_col: int, value, width: int) -> None:
    """Place value at 1-indexed start_col, left-justified, padded to width."""
    s = '' if value is None else str(value)
    s = s[:width].ljust(width)
    idx = start_col - 1
    buf[idx:idx + width] = list(s)


def build_report_line(record) -> str:
    """Build a single 167-char REPOTXT line matching the SAS PUT statement."""
    buf = [' '] * REPORT_LINE_WIDTH
    put_field(buf,   1, record.get('brabbr',   ''),  3)
    put_field(buf,   9, record.get('cac',      ''), 20)
    put_field(buf,  29, record.get('regno',    ''), 13)
    put_field(buf,  43, record.get('make',     ''), 16)
    put_field(buf,  60, record.get('model',    ''), 21)
    put_field(buf,  82, record.get('engine',   ''), 40)
    put_field(buf, 123, record.get('chassis',  ''), 40)
    put_field(buf, 164, record.get('collyear', ''),  4)
    return ''.join(buf)


# =====================================================================
# Step 1: date / period
# =====================================================================
def compute_report_dates() -> dict:
    """Derive REPTDATE = SYSDATE - 1 and all dependent macro variables."""
    reptdate = datetime.now() - timedelta(days=1)
    day, month, year = reptdate.day, reptdate.month, reptdate.year

    if day == 8:
        sdd, wk, wk1 = 1, '1', '4'
    elif day == 15:
        sdd, wk, wk1 = 9, '2', '1'
    elif day == 22:
        sdd, wk, wk1 = 16, '3', '2'
    else:
        sdd, wk, wk1 = 23, '4', '3'

    mm1 = (month - 1 or 12) if wk == '1' else month
    sdate = datetime(year, month, sdd)

    return {
        'reptdate':  reptdate,
        'sdate':     sdate,
        'wk':        wk,
        'wk1':       wk1,
        'reptmon':   f"{month:02d}",
        'reptmon1':  f"{mm1:02d}",
        'reptyear':  str(year),
        'reptday':   f"{day:02d}",
        # SAS PUT(REPTDATE, DDMMYY8.) -> 'dd/mm/yy'
        'rdate':     reptdate.strftime('%d/%m/%y'),
        'sdate_str': sdate.strftime('%d/%m/%y'),
    }


# =====================================================================
# Step 2-5: data loading
# =====================================================================
def read_lnnote_filtered(lnnote_path: Path) -> pd.DataFrame:
    """Read LNNOTE with column pruning + SAS WHERE filters."""
    print("Reading LNNOTE.sas7bdat (only required columns)...")
    df, _ = pyreadstat.read_sas7bdat(
        str(lnnote_path),
        usecols=['ACCTNO', 'LOANTYPE', 'NTBRCH', 'COLLDESC', 'COLLYEAR',
                 'BALANCE', 'BORSTAT'],
    )
    df.columns = df.columns.str.lower()
    print(f"  LNNOTE total records: {len(df):,}")

    lt_norm = normalize_code(df['loantype'])
    df = df[
        lt_norm.isin(HP_VALUES_STR)
        & (df['balance'] > 0)
        & (~df['borstat'].isin(['F', 'I', 'R']))
    ].copy()

    df = df[['acctno', 'loantype', 'ntbrch', 'colldesc', 'collyear']]
    print(f"  LNNOTE after filtering: {len(df):,}")
    return df


def read_name8(name8_path: Path) -> pd.DataFrame:
    """Read NAME8, keeping only acctno + engine (LINETHRE) + chassis (LINEFOUR)."""
    print("Reading NAME8.sas7bdat (only required columns)...")
    df, _ = pyreadstat.read_sas7bdat(
        str(name8_path),
        usecols=['ACCTNO', 'LINETHRE', 'LINEFOUR'],
    )
    df.columns = df.columns.str.lower()
    df = df.rename(columns={'linethre': 'engine', 'linefour': 'chassis'})
    print(f"  NAME8 records: {len(df):,}")
    return df


def read_loantemp(loantemp_path: Path) -> pd.DataFrame:
    """
    Read LOANTEMP. Keeps ACCTNO, ARREAR, plus ARREAR2 and DELQCD when available
    so we can auto-detect the correct arrears field if the SAS semantic differs
    from the direct `ARREAR >= 10` interpretation.
    """
    print("Reading LOANTEMP.sas7bdat...")
    # Try full column set first (for diagnostics); fall back to bare minimum
    try:
        df, meta = pyreadstat.read_sas7bdat(
            str(loantemp_path),
            usecols=['ACCTNO', 'ARREAR', 'ARREAR2', 'DELQCD', 'PRODUCT', 'BRANCH'],
        )
    except Exception:
        df, meta = pyreadstat.read_sas7bdat(
            str(loantemp_path),
            usecols=['ACCTNO', 'ARREAR'],
        )
    df.columns = df.columns.str.lower()
    print(f"  ARREAR records: {len(df):,}")

    # Quick diagnostic — how many rows pass the straightforward filter?
    arrear_ge10 = (df['arrear'] >= ARREAR_THRESHOLD).sum() if 'arrear' in df.columns else 0
    print(f"  ARREAR >= {ARREAR_THRESHOLD}: {arrear_ge10:,}")
    if 'arrear2' in df.columns:
        arrear2_ge10 = (df['arrear2'] >= ARREAR_THRESHOLD).sum()
        print(f"  ARREAR2 >= {ARREAR_THRESHOLD}: {arrear2_ge10:,}")

    return df


# =====================================================================
# Step 6-7: merge + derive
# =====================================================================
def merge_datasets(lnnote: pd.DataFrame,
                   name8: pd.DataFrame,
                   arrear: pd.DataFrame) -> pd.DataFrame:
    """LEFT JOIN LNNOTE with NAME8 and LOANTEMP on ACCTNO."""
    print("Merging datasets...")

    lnnote  = lnnote.copy()
    name8   = name8.copy()
    arrear  = arrear.copy()

    lnnote['acctno']  = normalize_acctno(lnnote['acctno'])
    name8['acctno']   = normalize_acctno(name8['acctno'])
    arrear['acctno']  = normalize_acctno(arrear['acctno'])

    # Keep only the columns we need from LOANTEMP for the join
    arrear_cols = ['acctno', 'arrear']
    if 'arrear2' in arrear.columns:
        arrear_cols.append('arrear2')

    merged = lnnote.merge(name8, on='acctno', how='left')
    merged = merged.merge(arrear[arrear_cols], on='acctno', how='left')
    print(f"  Merged REPO records: {len(merged):,}")

    # Coverage diagnostic
    n_lnnote  = lnnote['acctno'].nunique()
    n_matched = merged['arrear'].notna().sum()
    coverage  = 100.0 * n_matched / n_lnnote if n_lnnote else 0.0
    print(f"  LOANTEMP coverage of LNNOTE acctnos: {n_matched:,} / {n_lnnote:,} "
          f"({coverage:.1f}%)")
    return merged


def derive_repo_fields(merged: pd.DataFrame) -> pd.DataFrame:
    """Compute BRABBR/CAC via PBBELF formats, slice COLLDESC into fields."""
    print("Processing REPO data (BRABBR/CAC via PBBELF formats)...")
    records = []

    for _, r in merged.iterrows():
        ntbrch_int = to_int_code(r['ntbrch'])
        if ntbrch_int is not None:
            brabbr = PBBELF.format_brchcd(ntbrch_int) or '   '
            cac    = PBBELF.format_cacname(ntbrch_int) or 'NON CAC'
        else:
            brabbr, cac = '   ', 'NON CAC'

        coll_desc = str(r['colldesc']) if pd.notna(r['colldesc']) else ''
        make  = coll_desc[0:16]  if len(coll_desc) >= 16 else coll_desc.ljust(16)
        model = coll_desc[15:36] if len(coll_desc) >= 36 else coll_desc[15:] if len(coll_desc) > 15 else ''
        regno = coll_desc[39:52] if len(coll_desc) >= 52 else coll_desc[39:] if len(coll_desc) > 39 else ''

        engine   = str(r['engine'])   if pd.notna(r['engine'])   else ''
        chassis  = str(r['chassis'])  if pd.notna(r['chassis'])  else ''
        collyear = str(r['collyear'])[:4] if pd.notna(r['collyear']) else ''
        arrear   = float(r['arrear']) if pd.notna(r['arrear']) else 0.0
        arrear2  = float(r['arrear2']) if 'arrear2' in r and pd.notna(r['arrear2']) else None

        records.append({
            'acctno':   r['acctno'],
            'loantype': r['loantype'],
            'ntbrch':   ntbrch_int,
            'brabbr':   str(brabbr)[:3],
            'cac':      str(cac)[:20],
            'make':     make[:16],
            'model':    model[:21],
            'regno':    regno[:13],
            'engine':   engine[:40],
            'chassis':  chassis[:40],
            'collyear': collyear[:4],
            'arrear':   arrear,
            'arrear2':  arrear2,
        })

    REPO_COLUMNS = ['acctno', 'loantype', 'ntbrch', 'brabbr', 'cac', 'make',
                    'model', 'regno', 'engine', 'chassis', 'collyear',
                    'arrear', 'arrear2']
    out = pd.DataFrame(records, columns=REPO_COLUMNS)
    print(f"  Processed {len(out):,} records")
    return out


# =====================================================================
# Step 8-9: split + sort
# =====================================================================
def split_and_sort(repo: pd.DataFrame):
    """SAS: IF ARREAR GE 10; OUTPUT REPO; IF LOANTYPE IN (983,993) OUTPUT REPO1."""
    print(f"Splitting into REPO and REPO1 (ARREAR >= {ARREAR_THRESHOLD})...")
    repo_out = repo[repo['arrear'] >= ARREAR_THRESHOLD].copy()

    lt_norm = normalize_code(repo_out['loantype'])
    repo1_out = repo_out[lt_norm.isin(REPORT_LOANTYPES)].copy()

    print(f"  REPO  (ARREAR >= {ARREAR_THRESHOLD}): {len(repo_out):,}")
    print(f"  REPO1 (LOANTYPE {'/'.join(REPORT_LOANTYPES)}): {len(repo1_out):,}")

    print("Sorting by REGNO...")
    repo_out  = repo_out.sort_values('regno').reset_index(drop=True)
    repo1_out = repo1_out.sort_values('regno').reset_index(drop=True)
    print(f"  REPO sorted:  {len(repo_out):,}")
    print(f"  REPO1 sorted: {len(repo1_out):,}")

    # Sanity check: if REPO count is way above expected, warn (data-semantic issue)
    if len(repo_out) > EXPECTED_REPO_MAX:
        print(f"  ⚠  WARNING: REPO has {len(repo_out):,} records but expected "
              f"{EXPECTED_REPO_MIN}–{EXPECTED_REPO_MAX}.")
        print(f"     This likely means LOANTEMP.ARREAR uses coded/bucketed values,")
        print(f"     and 'ARREAR >= 10' is over-inclusive. Check with SAS owner")
        print(f"     whether the SAS filter is actually on ARREAR2 or DELQCD.")
    elif len(repo_out) < EXPECTED_REPO_MIN:
        print(f"  ⚠  WARNING: REPO has only {len(repo_out):,} records but expected "
              f"{EXPECTED_REPO_MIN}–{EXPECTED_REPO_MAX}.")
        print(f"     This likely means the LOANTEMP file does not cover the full")
        print(f"     HP portfolio (e.g. iccd2 only has ACCTNOs starting with 29).")

    return repo_out, repo1_out


# =====================================================================
# Step 10-11: write output files
# =====================================================================
def write_report(df: pd.DataFrame, path: Path, header: str) -> None:
    """Write a fixed-width REPOTXT/REPOTXT1 file."""
    print(f"Creating {path.name}...")
    with open(path, 'w') as f:
        if len(df) > 0:
            f.write(header + '\n')
        for _, record in df.iterrows():
            f.write(build_report_line(record) + '\n')
    print(f"  Wrote {len(df):,} records to {path}")


# =====================================================================
# Main
# =====================================================================
def main() -> int:
    print("EIMREPOI - Repossession Listing Report")
    print("=" * 60)

    # ---- Dates ----
    d = compute_report_dates()
    print(f"Processing date: {d['reptdate']}")
    print(f"Week: {d['wk']}, Previous Week: {d['wk1']}")
    print(f"Month: {d['reptmon']}, Previous Month: {d['reptmon1']}")
    print(f"RDate: {d['rdate']}, SDate: {d['sdate_str']}")

    # ---- Paths ----
    lnnote_path   = INPUT_BASE / "EIBRCGCS" / f"enrh_ln_note_m{d['reptmon']}.sas7bdat"
    name8_path    = INPUT_BASE / "EIMREPOI" / "stg_ln_nam8file.sas7bdat"
    loantemp_path = INPUT_BASE / "EIMREPOI" / "loantemp.sas7bdat"
    repotxt_path  = OUTPUT_BASE / "REPOTXT.txt"
    repotxt1_path = OUTPUT_BASE / "REPOTXT1.txt"

    OUTPUT_BASE.mkdir(parents=True, exist_ok=True)

    print(f"LNNOTE file:   {lnnote_path}")
    print(f"NAME8 file:    {name8_path}")
    print(f"LOANTEMP file: {loantemp_path}")
    print(f"HP loan types from PBBLNFMT ({len(HP_VALUES_INT)}): {HP_VALUES_INT}")
    print("=" * 60)

    # ---- Pre-flight ----
    for p in (lnnote_path, name8_path, loantemp_path):
        if not p.exists():
            print(f"ERROR: required input file not found: {p}", file=sys.stderr)
            return 1

    # ---- Read + filter ----
    lnnote   = read_lnnote_filtered(lnnote_path)
    name8    = read_name8(name8_path)
    arrear   = read_loantemp(loantemp_path)
    print("=" * 60)

    # ---- Merge + derive ----
    merged = merge_datasets(lnnote, name8, arrear)
    repo   = derive_repo_fields(merged)
    print("=" * 60)

    # ---- Split + sort ----
    repo_out, repo1_out = split_and_sort(repo)
    print("=" * 60)

    # ---- Write ----
    write_report(repo_out,  repotxt_path,  f"{d['rdate']}-REPOSSESSION LISTING")
    write_report(repo1_out, repotxt1_path, f"{d['rdate']}-REPOSSESSION LISTING (983,993)")
    print("=" * 60)

    # ---- Summary ----
    print("\nPROCESSING COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    print(f"  Processing Date: {d['reptdate'].strftime('%Y-%m-%d')}")
    print(f"  RDate:           {d['rdate']}")
    print(f"  SDate:           {d['sdate_str']}")
    print(f"  Week:            {d['wk']} (prev {d['wk1']})")
    print(f"  HP loan types:   {HP_VALUES_INT}")
    print(f"  LNNOTE filtered: {len(lnnote):,}")
    print(f"  REPO  records:   {len(repo_out):,}")
    print(f"  REPO1 records:   {len(repo1_out):,}")
    print(f"  Output:          {repotxt_path}")
    print(f"                   {repotxt1_path}")
    print("=" * 60)

    if len(repo_out) > 0:
        print("Loan type distribution in REPO:")
        for lt, cnt in repo_out['loantype'].value_counts().items():
            print(f"  {lt}: {cnt} records")
        print("=" * 60)
        print("Sample REPO (first 3 records):")
        for i in range(min(3, len(repo_out))):
            r = repo_out.iloc[i]
            print(f"  {i+1}. BRABBR={r['brabbr']}  CAC={r['cac']!r}  "
                  f"REGNO={r['regno']!r}  LOANTYPE={r['loantype']}  "
                  f"ARREAR={r['arrear']}  ARREAR2={r['arrear2']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
