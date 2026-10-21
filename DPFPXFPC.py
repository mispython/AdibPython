import polars as pl
import pyreadstat
from pathlib import Path
from typing import Optional
import datetime
import sys
import os

# TODO: adjust this path if PBBELF.py / PBBLNFMT.py live somewhere else
sys.path.insert(0, "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
import PBBELF
import PBBLNFMT

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
input_pbb_path  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS")
input_pibb_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS")
brh_dir         = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIHPTOP")
output_path     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMBRAS")
output_path.mkdir(parents=True, exist_ok=True)

LNNOTE_COLUMNS = [
    "ISSUEDT", "LOANTYPE", "PAIDIND", "BALANCE", "NTBRCH",
    "COLLDESC", "ACCTNO", "NAME", "ENTITY_CD",
]

CHUNKSIZE = 500_000

# ---------------------------------------------------------------------------
# HPD codes
# ---------------------------------------------------------------------------
_hpd_raw = os.environ.get("HPD_CODES", "").strip()
if _hpd_raw:
    HPD_VALUES = [int(v.strip()) for v in _hpd_raw.split(",") if v.strip()]
    print(f"HPD_CODES overridden from environment: {HPD_VALUES}")
else:
    HPD_VALUES = list(PBBLNFMT.HPD)
    print(f"Using HPD from PBBLNFMT.HPD: {HPD_VALUES}")

# ---------------------------------------------------------------------------
# Reporting date parameters
# Precedence:
#   1. REPTMON + REPTYEAR (explicit, e.g. REPTMON=08 REPTYEAR=2026)
#   2. REPTDATE_OVERRIDE (YYYY-MM-DD, applies SAS day-of-month bucketing)
#   3. today - 1 day
# ---------------------------------------------------------------------------
def compute_global_vars(reptdate: datetime.datetime) -> dict:
    day, month, year = reptdate.day, reptdate.month, reptdate.year
    if day == 8:
        sdd, wk, wk1 = 1, '1', '4'
    elif day == 15:
        sdd, wk, wk1 = 9, '2', '1'
    elif day == 22:
        sdd, wk, wk1 = 16, '3', '2'
    else:
        sdd, wk, wk1 = 23, '4', '3'

    mm  = month
    mm1 = (12 if month == 1 else month - 1) if wk == '1' else month
    sdate = datetime.datetime(year, month, sdd)

    return {
        'NOWK':     wk,
        'NOWK1':    wk1,
        'REPTMON':  f"{mm:02d}",
        'REPTMON1': f"{mm1:02d}",
        'REPTYEAR': str(year),
        'REPTDAY':  f"{day:02d}",
        'RDATE':    reptdate.strftime('%d%m%y'),
        'SDATE':    sdate.strftime('%d%m%y'),
    }


_reptmon_env  = os.environ.get("REPTMON", "").strip()
_reptyear_env = os.environ.get("REPTYEAR", "").strip()
_reptdate_env = os.environ.get("REPTDATE_OVERRIDE", "").strip()

if _reptmon_env and _reptyear_env:
    # Build a synthetic reptdate: pick day=30 so we get the "else" branch
    # (SDD=23, WK=4) — matches the standard month-end reporting bucket.
    reptdate = datetime.datetime(int(_reptyear_env), int(_reptmon_env), 28)
    print(f"Using explicit REPTMON={_reptmon_env} REPTYEAR={_reptyear_env}")
elif _reptdate_env:
    reptdate = datetime.datetime.strptime(_reptdate_env, "%Y-%m-%d")
    print(f"Using REPTDATE_OVERRIDE={_reptdate_env}")
else:
    reptdate = datetime.datetime.now() - datetime.timedelta(days=1)

global_vars = compute_global_vars(reptdate)
print("Global variables:", global_vars)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def parse_issuedt(df: pl.DataFrame, verbose: bool = False) -> pl.DataFrame:
    """Parse ISSUEDT defensively. Handles three encodings:

      1. Real SAS date (numeric, days since 1960-01-01) — small values like 14633
      2. Z11-encoded MDDYYYYDDD / MMDDYYYYDDD — large values like 9.7 billion
      3. String 'YYYY-MM-DD' or similar — string dtype

    Produces: ISSDT (pl.Date), ISSMTH (Int32), ISSYR (Int32)
    """
    src = df['ISSUEDT']

    # Case A: already a date/datetime
    if src.dtype in (pl.Date, pl.Datetime):
        if verbose:
            print(f"  [parse_issuedt] real date dtype ({src.dtype})")
        return df.with_columns([
            src.cast(pl.Date).alias('ISSDT'),
            src.dt.month().cast(pl.Int32).alias('ISSMTH'),
            src.dt.year().cast(pl.Int32).alias('ISSYR'),
        ])

    # Case B: string — try ISO first, fall back to Z11 numeric interpretation
    if src.dtype == pl.Utf8:
        if verbose:
            print(f"  [parse_issuedt] string dtype; attempting ISO parse")
        try:
            d = pl.col('ISSUEDT').str.strptime(pl.Date, "%Y-%m-%d", strict=False)
            return df.with_columns([
                d.alias('ISSDT'),
                d.dt.month().cast(pl.Int32).alias('ISSMTH'),
                d.dt.year().cast(pl.Int32).alias('ISSYR'),
            ])
        except Exception:
            pass
        # fall through to numeric path

    # Case C: numeric — decide between SAS date and Z11 by magnitude
    numeric = pl.col('ISSUEDT').cast(pl.Int64, strict=False)

    # Compute per-column: fraction of values < 1e6 (SAS-date heuristic)
    is_sas_date = numeric < 1_000_000
    frac_sas = is_sas_date.mean()

    if verbose:
        kind = "SAS date (days since 1960)" if (frac_sas or 0) > 0.5 else "Z11 encoded"
        print(f"  [parse_issuedt] numeric dtype; using {kind}")

    # Z11 path
    z11 = numeric.cast(pl.Utf8).str.pad_start(11, '0')
    z11_date = pl.date(
        z11.str.slice(4, 4).cast(pl.Int32, strict=False),
        z11.str.slice(0, 2).cast(pl.Int32, strict=False),
        z11.str.slice(2, 2).cast(pl.Int32, strict=False),
    )

    # SAS-date path
    sas_date = pl.date(1960, 1, 1) + pl.duration(days=numeric)

    date_expr = pl.when(is_sas_date).then(sas_date).otherwise(z11_date).alias('ISSDT')

    return df.with_columns([date_expr]).with_columns([
        pl.col('ISSDT').dt.month().cast(pl.Int32).alias('ISSMTH'),
        pl.col('ISSDT').dt.year().cast(pl.Int32).alias('ISSYR'),
    ])


def diagnose_chunk(pdf, reptmon: int, reptyear: int, label: str):
    """Print distribution stats for each filter column so we can see
    exactly which filter is rejecting rows."""
    print(f"\n{'='*70}")
    print(f"DIAGNOSTIC [{label}]  — rows in chunk: {len(pdf):,}")
    print(f"{'='*70}")
    print(f"columns: {list(pdf.columns)}")

    df = pl.from_pandas(pdf)

    print("\n--- LOANTYPE ---")
    print(f"dtype: {df['LOANTYPE'].dtype}")
    lt = df['LOANTYPE'].cast(pl.Int64, strict=False)
    print(f"nulls: {lt.null_count()}")
    uniq_lt = df.select(pl.col('LOANTYPE').cast(pl.Int64, strict=False).unique().sort())
    print(f"distinct count: {uniq_lt.height}")
    print(f"sample distinct (first 30): {uniq_lt.head(30).to_series().to_list()}")
    print(f"  in HPD_VALUES {HPD_VALUES}: {lt.is_in(HPD_VALUES).sum()}")

    print("\n--- PAIDIND ---")
    print(f"dtype: {df['PAIDIND'].dtype}")
    print(f"value counts (top 10): "
          f"{df.group_by('PAIDIND').len().sort('len', descending=True).head(10).to_dicts()}")

    print("\n--- BALANCE ---")
    print(f"dtype: {df['BALANCE'].dtype}")
    b = df['BALANCE'].cast(pl.Float64, strict=False)
    print(f"min: {b.min()}, max: {b.max()}, < 0: {b.lt(0).sum()}, > 0: {b.gt(0).sum()}")

    print("\n--- ENTITY_CD ---")
    print(f"dtype: {df['ENTITY_CD'].dtype}")
    print(f"value counts (top 10): "
          f"{df.group_by('ENTITY_CD').len().sort('len', descending=True).head(10).to_dicts()}")
    ec = df['ENTITY_CD'].cast(pl.Utf8).str.strip_chars()
    print(f"  == 'PIBB' (trim):    {df.filter(ec == 'PIBB').height}")
    print(f"  != 'PIBB' (trim):    {df.filter(ec != 'PIBB').height}")

    print("\n--- ISSUEDT ---")
    print(f"dtype: {df['ISSUEDT'].dtype}")
    print(f"sample raw (first 10): {df['ISSUEDT'].head(10).to_list()}")
    parsed = parse_issuedt(df, verbose=True)
    print(f"parsed ISSDT sample: {parsed['ISSDT'].head(5).to_list()}")
    print(f"ISSMTH distribution: {parsed.group_by('ISSMTH').len().sort('ISSMTH').to_dicts()}")
    print(f"ISSYR  distribution: {parsed.group_by('ISSYR').len().sort('ISSYR').to_dicts()}")
    print(f"  ISSMTH == {reptmon}: {parsed.filter(pl.col('ISSMTH') == reptmon).height}")
    print(f"  ISSYR  == {reptyear}: {parsed.filter(pl.col('ISSYR') == reptyear).height}")
    both = parsed.filter(
        (pl.col('ISSMTH') == reptmon) & (pl.col('ISSYR') == reptyear)
    ).height
    print(f"  BOTH (ISSMTH=={reptmon} & ISSYR=={reptyear}): {both}")

    print("\n--- FILTER FUNNEL (conventional) ---")
    parsed = parse_issuedt(df)
    ec_trim = pl.col('ENTITY_CD').cast(pl.Utf8).str.strip_chars()
    lt_i64  = pl.col('LOANTYPE').cast(pl.Int64, strict=False)
    paid_s  = pl.col('PAIDIND').cast(pl.Utf8).str.strip_chars().str.to_uppercase()
    bal_f   = pl.col('BALANCE').cast(pl.Float64, strict=False)

    n0 = parsed.height
    n1 = parsed.filter(ec_trim != 'PIBB').height
    n2 = parsed.filter((ec_trim != 'PIBB') & lt_i64.is_in(HPD_VALUES)).height
    n3 = parsed.filter((ec_trim != 'PIBB') & lt_i64.is_in(HPD_VALUES) & (paid_s != 'P')).height
    n4 = parsed.filter((ec_trim != 'PIBB') & lt_i64.is_in(HPD_VALUES) & (paid_s != 'P') & (bal_f > 0)).height
    n5 = parsed.filter(
        (ec_trim != 'PIBB') & lt_i64.is_in(HPD_VALUES) & (paid_s != 'P') & (bal_f > 0)
        & (pl.col('ISSMTH') == reptmon) & (pl.col('ISSYR') == reptyear)
    ).height
    print(f"  total rows:                              {n0:,}")
    print(f"  after ENTITY_CD != 'PIBB':               {n1:,}")
    print(f"  after LOANTYPE in HPD:                   {n2:,}")
    print(f"  after PAIDIND != 'P':                    {n3:,}")
    print(f"  after BALANCE > 0:                       {n4:,}")
    print(f"  after ISSMTH=={reptmon} & ISSYR=={reptyear}:           {n5:,}")

    print(f"{'='*70}\n")


def filter_lnnote_chunk(pdf, reptmon: int, reptyear: int, entity_filter: str) -> pl.DataFrame:
    """Apply ENTITY_CD split + LOANTYPE/PAIDIND/BALANCE/ISSUE-month filters."""
    if pdf.empty:
        return pl.DataFrame()

    df = pl.from_pandas(pdf)
    df = parse_issuedt(df)

    ec   = pl.col('ENTITY_CD').cast(pl.Utf8).str.strip_chars()
    lt   = pl.col('LOANTYPE').cast(pl.Int64, strict=False)
    paid = pl.col('PAIDIND').cast(pl.Utf8).str.strip_chars().str.to_uppercase()
    bal  = pl.col('BALANCE').cast(pl.Float64, strict=False)

    if entity_filter == "conventional":
        df = df.filter(ec != 'PIBB')
    elif entity_filter == "islamic":
        df = df.filter(ec == 'PIBB')
    else:
        raise ValueError(f"Unknown entity_filter: {entity_filter}")

    df = df.filter(
        lt.is_in(HPD_VALUES)
        & (paid != 'P')
        & (bal > 0)
        & (pl.col('ISSMTH') == reptmon)
        & (pl.col('ISSYR') == reptyear)
    )
    return df


def read_lnnote(filepath: Path, reptmon: int, reptyear: int,
                entity_filter: str, chunksize: int = CHUNKSIZE,
                diagnose: bool = False) -> pl.DataFrame:
    """Stream a .sas7bdat file in chunks, filter immediately."""
    if not filepath.exists():
        print(f"File not found, skipping: {filepath}")
        return pl.DataFrame()

    filtered_chunks = []
    reader = pyreadstat.read_file_in_chunks(
        pyreadstat.read_sas7bdat,
        str(filepath),
        chunksize=chunksize,
        usecols=LNNOTE_COLUMNS,
    )

    total_rows = 0
    first = True
    for pdf_chunk, _meta in reader:
        total_rows += len(pdf_chunk)
        if first and diagnose:
            diagnose_chunk(pdf_chunk, reptmon, reptyear,
                           label=f"{filepath.name} [{entity_filter}]")
        first = False
        filtered = filter_lnnote_chunk(pdf_chunk, reptmon, reptyear, entity_filter)
        if filtered.height > 0:
            filtered_chunks.append(filtered)

    kept = sum(c.height for c in filtered_chunks)
    print(f"{filepath.name} [{entity_filter}]: scanned {total_rows:,} rows, "
          f"kept {kept:,} after filter")

    if filtered_chunks:
        return pl.concat(filtered_chunks, how="diagonal")
    return pl.DataFrame()


def find_lnnote_file(base_dir: Path, reptmon: str) -> Optional[Path]:
    """Try several naming conventions for the LNNOTE file.

    Your current data uses enrh_ln_note_m{MM}.sas7bdat. Other conventions
    are tried so the script keeps working if the naming changes.
    """
    candidates = [
        base_dir / f"enrh_ln_note_m{reptmon}.sas7bdat",
        base_dir / f"enrh_ln_note_m{int(reptmon)}.sas7bdat",
        base_dir / f"lnnote_m{reptmon}.sas7bdat",
        base_dir / "enrh_ln_note.sas7bdat",
        base_dir / "lnnote.sas7bdat",
        base_dir / "ln_note.sas7bdat",
    ]
    for c in candidates:
        if c.exists():
            return c

    # Fallback: glob for anything matching *_ln*note*.sas7bdat
    matches = sorted(base_dir.glob("*ln*note*.sas7bdat"))
    if matches:
        print(f"WARNING: {candidates[0].name} not found; using {matches[0].name}")
        return matches[0]
    return None


def _resolve_lkp_branch_path(brh_dir: Path) -> Optional[Path]:
    candidates = [
        brh_dir / "LKP_BRANCH",
        brh_dir / "LKP_BRANCH.sas7bdat",
        brh_dir / "LKP_BRANCH" / "lkp_branch.sas7bdat",
        brh_dir / "LKP_BRANCH" / "LKP_BRANCH.sas7bdat",
        brh_dir / "lkp_branch.sas7bdat",
    ]
    for c in candidates:
        if c.exists() and c.is_file():
            return c
    return None


def read_brh_lookup(brh_dir: Path) -> pl.DataFrame:
    """Read LKP_BRANCH; fall back to PBBELF.BRCHCD_MAP."""
    lkp_file = _resolve_lkp_branch_path(brh_dir)
    if lkp_file is not None:
        try:
            brh_df, _ = pyreadstat.read_sas7bdat(str(lkp_file))
            brh = pl.from_pandas(brh_df)
            brh = brh.rename({c: c.upper() for c in brh.columns})
            if 'BRABBR' not in brh.columns:
                for cand in ('BRCHNAME', 'BRANCH_NAME', 'BRNM'):
                    if cand in brh.columns:
                        brh = brh.rename({cand: 'BRABBR'})
                        break
            brh = brh.select([
                pl.col('BRANCH').cast(pl.Int64),
                pl.col('BRABBR').cast(pl.Utf8),
            ])
            print(f"Loaded branch lookup from {lkp_file}: {brh.height} rows")
            return brh
        except Exception as exc:
            print(f"WARNING: failed to read {lkp_file} ({exc}); "
                  f"falling back to PBBELF.BRCHCD_MAP")

    brh_data = [{"BRANCH": k, "BRABBR": v} for k, v in PBBELF.BRCHCD_MAP.items()]
    brh = pl.DataFrame(brh_data)
    print(f"Using PBBELF.BRCHCD_MAP as branch lookup: {brh.height} rows")
    return brh


# ---------------------------------------------------------------------------
# *** A/C RELEASED FOR THE MTH ***
# ---------------------------------------------------------------------------
reptmon_str  = global_vars['REPTMON']
reptmon_int  = int(reptmon_str)
reptyear_int = int(global_vars['REPTYEAR'])

DIAGNOSE = os.environ.get("DIAGNOSE", "0") == "1"
print(f"Looking for LNNOTE file for REPTMON={reptmon_str}, "
      f"REPTYEAR={reptyear_int}, DIAGNOSE={DIAGNOSE}")

lnnote_file_pbb = find_lnnote_file(input_pbb_path, reptmon_str)
if lnnote_file_pbb is None:
    print(f"ERROR: no LNNOTE file found under {input_pbb_path}")
    sys.exit(1)
print(f"Reading LNNOTE file: {lnnote_file_pbb}")

lnnote_pbb = read_lnnote(
    lnnote_file_pbb,
    reptmon_int, reptyear_int, "conventional",
    diagnose=DIAGNOSE,
)

# Islamic file may or may not be separate; if same path, reuse result
lnnote_file_pibb = find_lnnote_file(input_pibb_path, reptmon_str)
if lnnote_file_pibb is None:
    print(f"WARNING: no LNNOTE file found under {input_pibb_path}; skipping PIBB")
    lnnote_pibb = pl.DataFrame()
elif lnnote_file_pibb == lnnote_file_pbb:
    print(f"NOTE: PIBB path resolves to same file as conventional; "
          f"reading it again for the PIBB filter")
    lnnote_pibb = read_lnnote(
        lnnote_file_pibb,
        reptmon_int, reptyear_int, "islamic",
        diagnose=False,
    )
else:
    print(f"Reading LNNOTE file (PIBB): {lnnote_file_pibb}")
    lnnote_pibb = read_lnnote(
        lnnote_file_pibb,
        reptmon_int, reptyear_int, "islamic",
        diagnose=False,
    )

lnnote_combined = pl.concat([lnnote_pbb, lnnote_pibb], how="diagonal")

if not lnnote_combined.is_empty():
    loan_df = lnnote_combined.with_columns([
        pl.when(pl.col('COLLDESC').cast(pl.Utf8).str.slice(37, 1).is_in(['N', 'R']))
        .then(pl.lit('N'))
        .otherwise(pl.lit('S'))
        .alias('NEWSEC'),
        pl.col('NTBRCH').cast(pl.Int64, strict=False)
        .map_elements(PBBELF.format_brchcd, return_dtype=pl.Utf8)
        .alias('BRABBR'),
    ])

    brh_df = read_brh_lookup(brh_dir).filter(
        (pl.col('BRANCH') < 900)
        & (~pl.col('BRANCH').is_in([1, 99, 100, 218]))
    )

    nobr    = brh_df.height
    noacct  = loan_df.height
    noacrel = round(noacct * 0.3 / nobr) if nobr > 0 else 0
    print(f"Number of branches: {nobr}")
    print(f"Number of accounts: {noacct}")
    print(f"Average accounts per branch: {noacrel}")

    loan11_df = (
        loan_df.group_by('BRABBR')
        .agg(pl.len().alias('NOACCT'))
        .with_columns(
            pl.col('NOACCT').mul(0.3).round().cast(pl.Int64).alias('AVGNO_temp')
        )
        .with_columns(
            pl.when(pl.col('AVGNO_temp') < noacrel)
            .then(pl.lit(noacrel))
            .otherwise(pl.col('AVGNO_temp'))
            .alias('AVGNO')
        )
        .drop('AVGNO_temp')
    )

    loan1_df = loan_df.join(loan11_df, on='BRABBR', how='left')

    loan_sorted = loan1_df.sort('BRABBR')

    output_lines = []
    current_brabb = None
    avgacc = 0

    for row in loan_sorted.iter_rows(named=True):
        if row['BRABBR'] != current_brabb:
            current_brabb = row['BRABBR']
            avgacc = 0

        avgacc += 1
        if avgacc <= row['AVGNO']:
            issmth = f"{row['ISSMTH']:02d}"
            issyr  = f"{row['ISSYR']}"
            acctno = str(row.get('ACCTNO', '')).ljust(10)[:10]
            name   = str(row.get('NAME',   '')).ljust(30)[:30]
            issdt  = row['ISSDT'].strftime('%d%m%Y')
            brabb  = str(row.get('BRABBR', '')).ljust(3)[:3]
            newsec = str(row.get('NEWSEC', ''))

            output_lines.append(
                f"{issmth}{issyr}{acctno}{name}{issdt}{brabb}{newsec}"
            )

    with open(output_path / "BRTXT1.txt", "w") as f:
        f.write("\n".join(output_lines))
    print(f"Generated BRTXT1.txt with {len(output_lines)} records")

    loan_df.write_csv(output_path / "LOAN.txt",   separator="|")
    loan11_df.write_csv(output_path / "LOAN11.txt", separator="|")
    loan1_df.write_csv(output_path / "LOAN1.txt",  separator="|")

else:
    print("\n*** No LNNOTE data found after filtering ***")
    print("Run with DIAGNOSE=1 to see which filter dropped rows, or try:")
    print("  REPTMON=08 REPTYEAR=2026 python EIBMBRAS.py")
    print("  REPTDATE_OVERRIDE=2026-08-30 python EIBMBRAS.py")

print("Processing completed!")
