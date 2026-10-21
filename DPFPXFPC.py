import polars as pl
import pyreadstat
from pathlib import Path
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

# Also expose HP for reference during diagnostics
HP_VALUES = list(getattr(PBBLNFMT, "HP", HPD_VALUES))

# ---------------------------------------------------------------------------
# Reporting date parameters
# ---------------------------------------------------------------------------
_override = os.environ.get("REPTDATE_OVERRIDE", "").strip()
if _override:
    reptdate = datetime.datetime.strptime(_override, "%Y-%m-%d")
else:
    reptdate = datetime.datetime.now() - datetime.timedelta(days=1)

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

global_vars = {
    'NOWK':     wk,
    'NOWK1':    wk1,
    'REPTMON':  f"{mm:02d}",
    'REPTMON1': f"{mm1:02d}",
    'REPTYEAR': str(year),
    'REPTDAY':  f"{day:02d}",
    'RDATE':    reptdate.strftime('%d%m%y'),
    'SDATE':    sdate.strftime('%d%m%y'),
}

print("Global variables:", global_vars)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def parse_issuedt(df: pl.DataFrame) -> pl.DataFrame:
    """Parse ISSUEDT defensively. Handles:
      - numeric MDDYYYYDDD / MMDDYYYYDDD (Z11 style)
      - real SAS date (stored as days since 1960) — detected by small value
      - string 'YYYY-MM-DD' or 'MM/DD/YYYY'

    Produces: ISSDT (pl.Date), ISSMTH (Int32), ISSYR (Int32)
    """
    src = df['ISSUEDT']

    # Case A: it's already a pl.Date / pl.Datetime (pyreadstat converts these)
    if src.dtype in (pl.Date, pl.Datetime):
        return df.with_columns([
            src.cast(pl.Date).alias('ISSDT'),
            src.dt.month().cast(pl.Int32).alias('ISSMTH'),
            src.dt.year().cast(pl.Int32).alias('ISSYR'),
        ])

    # Case B: string column — try ISO-ish parse first
    if src.dtype == pl.Utf8:
        # Try 'YYYY-MM-DD'
        try:
            d = pl.col('ISSUEDT').str.strptime(pl.Date, "%Y-%m-%d", strict=False)
            return df.with_columns([
                d.alias('ISSDT'),
                d.dt.month().cast(pl.Int32).alias('ISSMTH'),
                d.dt.year().cast(pl.Int32).alias('ISSYR'),
            ])
        except Exception:
            pass
        # Fall through to Z11 numeric handling below

    # Case C: numeric — could be Z11 encoded OR real SAS date (days since 1960)
    numeric = pl.col('ISSUEDT').cast(pl.Int64, strict=False)
    # SAS date offset: 1960-01-01 = day 0. A modern date (2026) ≈ 24xxx days.
    # Z11 encoded values are ~10^9..10^10. So a threshold of 10^6 cleanly
    # separates "real SAS date" from "Z11 string".
    is_sas_date = numeric < 1_000_000

    # Build both possibilities and choose with when/then
    # Z11 path
    z11 = (
        numeric.cast(pl.Utf8)
        .str.pad_start(11, '0')
    )
    z11_mm = z11.str.slice(0, 2).cast(pl.Int32, strict=False)
    z11_dd = z11.str.slice(2, 2).cast(pl.Int32, strict=False)
    z11_yy = z11.str.slice(4, 4).cast(pl.Int32, strict=False)
    z11_date = pl.date(z11_yy, z11_mm, z11_dd)

    # SAS-date path (days since 1960-01-01)
    sas_date = pl.date(1960, 1, 1) + pl.duration(days=numeric)

    is_sdt = (
        pl.when(is_sas_date).then(sas_date).otherwise(z11_date)
        .alias('ISSDT')
    )

    return df.with_columns([
        is_sdt,
    ]).with_columns([
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

    # --- LOANTYPE ---
    print("\n--- LOANTYPE ---")
    print(f"dtype: {df['LOANTYPE'].dtype}")
    lt = df['LOANTYPE'].cast(pl.Int64, strict=False)
    print(f"nulls: {lt.null_count()}")
    uniq_lt = df.select(pl.col('LOANTYPE').cast(pl.Int64, strict=False).unique().sort())
    print(f"distinct count: {uniq_lt.height}")
    print(f"sample distinct (first 30): {uniq_lt.head(30).to_series().to_list()}")
    print(f"  in HPD_VALUES {HPD_VALUES}: {lt.is_in(HPD_VALUES).sum()}")
    print(f"  in HP_VALUES  {HP_VALUES}:  {lt.is_in(HP_VALUES).sum()}")

    # --- PAIDIND ---
    print("\n--- PAIDIND ---")
    print(f"dtype: {df['PAIDIND'].dtype}")
    print(f"value counts (top 10): "
          f"{df.group_by('PAIDIND').len().sort('len', descending=True).head(10).to_dicts()}")
    print(f"  != 'P' (raw): {df.filter(pl.col('PAIDIND') != 'P').height}")
    print(f"  != 'P' (trim+upper): "
          f"{df.filter(pl.col('PAIDIND').cast(pl.Utf8).str.strip_chars().str.to_uppercase() != 'P').height}")

    # --- BALANCE ---
    print("\n--- BALANCE ---")
    print(f"dtype: {df['BALANCE'].dtype}")
    b = df['BALANCE'].cast(pl.Float64, strict=False)
    print(f"nulls: {b.null_count()}")
    print(f"min: {b.min()}, max: {b.max()}")
    print(f"  < 0: {b.lt(0).sum()}")
    print(f"  == 0: {b.eq(0).sum()}")
    print(f"  > 0: {b.gt(0).sum()}")

    # --- ENTITY_CD ---
    print("\n--- ENTITY_CD ---")
    print(f"dtype: {df['ENTITY_CD'].dtype}")
    print(f"value counts (top 10): "
          f"{df.group_by('ENTITY_CD').len().sort('len', descending=True).head(10).to_dicts()}")
    ec = df['ENTITY_CD'].cast(pl.Utf8)
    print(f"  == 'PIBB' (exact):    {df.filter(ec == 'PIBB').height}")
    print(f"  == 'PIBB' (trim):     {df.filter(ec.str.strip_chars() == 'PIBB').height}")
    print(f"  != 'PIBB' (trim):     {df.filter(ec.str.strip_chars() != 'PIBB').height}")

    # --- ISSUEDT ---
    print("\n--- ISSUEDT ---")
    print(f"dtype: {df['ISSUEDT'].dtype}")
    print(f"sample raw (first 10): {df['ISSUEDT'].head(10).to_list()}")
    try:
        parsed = parse_issuedt(df)
        print(f"parsed ISSDT  sample: {parsed['ISSDT'].head(5).to_list()}")
        print(f"parsed ISSMTH sample: {parsed['ISSMTH'].head(10).to_list()}")
        print(f"parsed ISSYR  sample: {parsed['ISSYR'].head(10).to_list()}")
        print(f"  ISSMTH distribution: "
              f"{parsed.group_by('ISSMTH').len().sort('ISSMTH').to_dicts()}")
        print(f"  ISSYR  distribution: "
              f"{parsed.group_by('ISSYR').len().sort('ISSYR').to_dicts()}")
        print(f"  ISSMTH == {reptmon}: {parsed.filter(pl.col('ISSMTH') == reptmon).height}")
        print(f"  ISSYR  == {reptyear}: {parsed.filter(pl.col('ISSYR') == reptyear).height}")
        both = parsed.filter(
            (pl.col('ISSMTH') == reptmon) & (pl.col('ISSYR') == reptyear)
        ).height
        print(f"  BOTH (ISSMTH=={reptmon} & ISSYR=={reptyear}): {both}")
    except Exception as e:
        print(f"  PARSE FAILED: {type(e).__name__}: {e}")

    # --- Combined funnel ---
    print("\n--- FILTER FUNNEL (conventional, i.e. ENTITY_CD != 'PIBB' trim) ---")
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

    # Defensive: trim entity code and normalise
    ec = pl.col('ENTITY_CD').cast(pl.Utf8).str.strip_chars()
    lt = pl.col('LOANTYPE').cast(pl.Int64, strict=False)
    paid = pl.col('PAIDIND').cast(pl.Utf8).str.strip_chars().str.to_uppercase()
    bal = pl.col('BALANCE').cast(pl.Float64, strict=False)

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
    """Stream a .sas7bdat file in chunks, filter immediately, print
    diagnostics on the first chunk."""
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


def _resolve_lkp_branch_path(brh_dir: Path):
    """LKP_BRANCH may be a file or a directory containing a .sas7bdat."""
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

lnnote_filename = f"enrh_ln_note_m{reptmon_str}.sas7bdat"
print(f"Reading LNNOTE file: {lnnote_filename}")

# Set diagnose=True to see detailed breakdown on the first chunk.
DIAGNOSE = os.environ.get("DIAGNOSE", "1") == "1"

lnnote_pbb = read_lnnote(
    input_pbb_path / lnnote_filename,
    reptmon_int, reptyear_int, "conventional",
    diagnose=DIAGNOSE,
)
lnnote_pibb = read_lnnote(
    input_pibb_path / lnnote_filename,
    reptmon_int, reptyear_int, "islamic",
    diagnose=False,   # already diagnosed the file on the conventional pass
)

lnnote_combined = pl.concat([lnnote_pbb, lnnote_pibb], how="diagonal")

if not lnnote_combined.is_empty():
    loan_df = lnnote_combined.with_columns([
        # NEWSEC from COLLDESC position 38 (SAS 1-based -> python idx 37)
        pl.when(pl.col('COLLDESC').cast(pl.Utf8).str.slice(37, 1).is_in(['N', 'R']))
        .then(pl.lit('N'))
        .otherwise(pl.lit('S'))
        .alias('NEWSEC'),
        # BRABBR from NTBRCH via PBBELF.format_brchcd
        pl.col('NTBRCH').cast(pl.Int64, strict=False)
        .map_elements(PBBELF.format_brchcd, return_dtype=pl.Utf8)
        .alias('BRABBR'),
    ])

    # *** AVERAGE PER BR BASE ON 30% OF TOTAL ***
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

    # *** COMPARE RELEASE PER BR AND AVERAGE PER BR ***
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

    # *** GENERATE TEXT FILE TO LOTUS NOTES SERVER ***
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
    print("Look at the DIAGNOSTIC block above to see which filter dropped the rows.")
    print("Common fixes:")
    print("  - REPTDAY/REPTMON: try REPTDATE_OVERRIDE=2026-08-31")
    print("  - HPD_CODES: try HP_VALUES from PBBLNFMT instead of HPD")
    print("  - ENTITY_CD: check whitespace/casing in the value counts")

print("Processing completed!")
