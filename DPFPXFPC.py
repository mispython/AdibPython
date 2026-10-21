import polars as pl
import pyreadstat
from pathlib import Path
from typing import Optional
import datetime
import sys
import os

sys.path.insert(0, "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
import PBBELF
import PBBLNFMT

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
input_pbb_path  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/a_lnnote")
input_pibb_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/a_lnnote")
brh_dir         = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIHPTOP")
output_path     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMBRAS")
output_path.mkdir(parents=True, exist_ok=True)

LNNOTE_COLUMNS = [
    "ISSUEDT", "LOANTYPE", "PAIDIND", "BALANCE", "NTBRCH",
    "COLLDESC", "ACCTNO", "NAME", "ENTITY_CD",
]

CHUNKSIZE = 1_000_000
TEST_ROW_LIMIT = None   # set to 100_000 for quick test, None for full scan

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
# Reporting date
# ---------------------------------------------------------------------------
_reptmon_env  = os.environ.get("REPTMON",  "").strip()
_reptyear_env = os.environ.get("REPTYEAR", "").strip()
_reptdate_env = os.environ.get("REPTDATE_OVERRIDE", "").strip()

if _reptmon_env and _reptyear_env:
    reptdate = datetime.datetime(int(_reptyear_env), int(_reptmon_env), 28)
elif _reptdate_env:
    reptdate = datetime.datetime.strptime(_reptdate_env, "%Y-%m-%d")
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
# ISSUEDT parser
# ---------------------------------------------------------------------------
def parse_issuedt(df: pl.DataFrame) -> pl.DataFrame:
    src = df['ISSUEDT']

    if src.dtype in (pl.Date, pl.Datetime):
        return df.with_columns(
            src.cast(pl.Date).alias('ISSDT')
        ).with_columns(
            pl.col('ISSDT').dt.month().cast(pl.Int32).alias('ISSMTH'),
            pl.col('ISSDT').dt.year().cast(pl.Int32).alias('ISSYR'),
        )

    if src.dtype == pl.Utf8:
        try:
            d = pl.col('ISSUEDT').str.strptime(pl.Date, "%Y-%m-%d", strict=False)
            return df.with_columns(d.alias('ISSDT')).with_columns(
                pl.col('ISSDT').dt.month().cast(pl.Int32).alias('ISSMTH'),
                pl.col('ISSDT').dt.year().cast(pl.Int32).alias('ISSYR'),
            )
        except Exception:
            pass

    n = pl.col('ISSUEDT').cast(pl.Int64, strict=False)
    is_sas = n < 1_000_000
    z11 = n.cast(pl.Utf8).str.pad_start(11, '0')
    return df.with_columns(
        pl.when(is_sas)
          .then(pl.date(1960, 1, 1) + pl.duration(days=n))
          .otherwise(pl.date(
              z11.str.slice(4, 4).cast(pl.Int32, strict=False),
              z11.str.slice(0, 2).cast(pl.Int32, strict=False),
              z11.str.slice(2, 2).cast(pl.Int32, strict=False),
          ))
          .alias('ISSDT')
    ).with_columns(
        pl.col('ISSDT').dt.month().cast(pl.Int32).alias('ISSMTH'),
        pl.col('ISSDT').dt.year().cast(pl.Int32).alias('ISSYR'),
    )


# ---------------------------------------------------------------------------
# Chunk filter
# ---------------------------------------------------------------------------
def filter_lnnote_chunk(pdf, reptmon: int, reptyear: int,
                        entity_filter: str) -> pl.DataFrame:
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


# ---------------------------------------------------------------------------
# Full-file streamed read
# ---------------------------------------------------------------------------
def read_lnnote(filepath: Path, reptmon: int, reptyear: int,
                entity_filter: str, chunksize: int = CHUNKSIZE) -> pl.DataFrame:
    if not filepath.exists():
        print(f"File not found, skipping: {filepath}")
        return pl.DataFrame()

    filtered_chunks = []
    reader = pyreadstat.read_file_in_chunks(
        pyreadstat.read_sas7bdat, str(filepath),
        chunksize=chunksize, usecols=LNNOTE_COLUMNS,
    )

    total_rows = 0
    for pdf_chunk, _meta in reader:
        total_rows += len(pdf_chunk)
        f = filter_lnnote_chunk(pdf_chunk, reptmon, reptyear, entity_filter)
        if f.height > 0:
            filtered_chunks.append(f)

    kept = sum(c.height for c in filtered_chunks)
    print(f"{filepath.name} [{entity_filter}]: scanned {total_rows:,} rows, "
          f"kept {kept:,}")

    if filtered_chunks:
        return pl.concat(filtered_chunks, how="diagonal")
    return pl.DataFrame()


# ---------------------------------------------------------------------------
# LKP_BRANCH reader — FIXED-WIDTH FLAT FILE
# SAS: INFILE BRHFILE LRECL=80;
#      INPUT @2 BRANCH 3. @6 BRABBR $3.;
# Positions (1-based): BRANCH = cols 2-4, BRABBR = cols 6-8
# Python indices:      BRANCH = line[1:4], BRABBR = line[5:8]
# ---------------------------------------------------------------------------
def find_lkp_branch(brh_dir: Path) -> Optional[Path]:
    candidates = [
        brh_dir / "LKP_BRANCH",
        brh_dir / "LKP_BRANCH.dat",
        brh_dir / "LKP_BRANCH.txt",
        brh_dir / "lkp_branch",
        brh_dir / "lkp_branch.dat",
        brh_dir / "lkp_branch.txt",
    ]
    for c in candidates:
        if c.exists() and c.is_file():
            return c
    # Last resort: any file starting with "lkp_branch" (case-insensitive)
    for p in brh_dir.iterdir():
        if p.is_file() and p.name.lower().startswith("lkp_branch"):
            return p
    return None


def read_brh_lookup_flat(lkp: Path) -> pl.DataFrame:
    """Parse a fixed-width LKP_BRANCH file per the SAS INFILE layout."""
    rows = []
    with open(lkp, "r", encoding="latin-1") as f:
        for lineno, line in enumerate(f, start=1):
            line = line.rstrip("\n\r")
            if len(line) < 8:
                continue
            branch_str = line[1:4].strip()
            brabbr_str = line[5:8].strip()
            try:
                branch_int = int(branch_str)
            except ValueError:
                continue   # header line or garbage; skip silently
            if not brabbr_str:
                continue
            rows.append({"BRANCH": branch_int, "BRABBR": brabbr_str})

    if not rows:
        print(f"WARNING: flat file {lkp} produced 0 valid rows")
        return pl.DataFrame()

    brh = pl.DataFrame(rows, schema={"BRANCH": pl.Int64, "BRABBR": pl.Utf8})
    print(f"Loaded branch lookup (flat file) from {lkp}: {brh.height} rows")
    return brh


def read_brh_lookup(brh_dir: Path) -> pl.DataFrame:
    lkp = find_lkp_branch(brh_dir)
    if lkp is not None:
        try:
            return read_brh_lookup_flat(lkp)
        except Exception as exc:
            print(f"WARNING: failed to read {lkp} ({exc}); using BRCHCD_MAP")

    brh = pl.DataFrame([{"BRANCH": k, "BRABBR": v}
                        for k, v in PBBELF.BRCHCD_MAP.items()],
                       schema={"BRANCH": pl.Int64, "BRABBR": pl.Utf8})
    print(f"Using PBBELF.BRCHCD_MAP as branch lookup: {brh.height} rows")
    return brh


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
reptmon_str  = global_vars['REPTMON']
reptmon_int  = int(reptmon_str)
reptyear_int = int(global_vars['REPTYEAR'])

lnnote_file = input_pbb_path / f"enrh_ln_note_m{reptmon_str}.sas7bdat"
if not lnnote_file.exists():
    for alt in [
        input_pbb_path / f"enrh_ln_note_m{int(reptmon_str)}.sas7bdat",
        input_pbb_path / "enrh_ln_note.sas7bdat",
        input_pbb_path / "lnnote.sas7bdat",
    ]:
        if alt.exists():
            lnnote_file = alt
            break

if not lnnote_file.exists():
    print(f"ERROR: LNNOTE file not found in {input_pbb_path}")
    sys.exit(1)

print(f"\nReading LNNOTE: {lnnote_file}")
print(f"Target month: {reptyear_int}-{reptmon_int:02d}   "
      f"(TEST_ROW_LIMIT={TEST_ROW_LIMIT})")

# Read conventional + islamic
if TEST_ROW_LIMIT is not None:
    reader = pyreadstat.read_file_in_chunks(
        pyreadstat.read_sas7bdat, str(lnnote_file),
        chunksize=min(TEST_ROW_LIMIT, 100_000), usecols=LNNOTE_COLUMNS,
    )
    parts, total = [], 0
    for pdf, _ in reader:
        if total >= TEST_ROW_LIMIT:
            break
        take = min(len(pdf), TEST_ROW_LIMIT - total)
        parts.append(pdf.iloc[:take])
        total += take
    import pandas as pd
    raw_pdf = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    conv_df = filter_lnnote_chunk(raw_pdf, reptmon_int, reptyear_int, "conventional")
    islm_df = filter_lnnote_chunk(raw_pdf, reptmon_int, reptyear_int, "islamic")
    print(f"TEST MODE: scanned {total:,} rows; "
          f"conventional={conv_df.height:,}, islamic={islm_df.height:,}")
else:
    conv_df = read_lnnote(lnnote_file, reptmon_int, reptyear_int, "conventional")
    islm_df = read_lnnote(lnnote_file, reptmon_int, reptyear_int, "islamic")

lnnote_combined = pl.concat([conv_df, islm_df], how="diagonal")
print(f"Combined filtered rows: {lnnote_combined.height:,}")

if lnnote_combined.is_empty():
    print("\n*** No data after filtering. ***")
    print("Try a different month:")
    print("  REPTMON=08 REPTYEAR=2026 python EIBMBRAS.py")
    sys.exit(0)

# ---------------------------------------------------------------------------
# LOAN dataset
# ---------------------------------------------------------------------------
loan_df = lnnote_combined.with_columns([
    pl.when(pl.col('COLLDESC').cast(pl.Utf8).str.slice(37, 1).is_in(['N', 'R']))
      .then(pl.lit('N')).otherwise(pl.lit('S')).alias('NEWSEC'),
    pl.col('NTBRCH').cast(pl.Int64, strict=False)
      .map_elements(PBBELF.format_brchcd, return_dtype=pl.Utf8).alias('BRABBR'),
])

# ---------------------------------------------------------------------------
# Branch lookup (flat file) + filter
# ---------------------------------------------------------------------------
brh_df = read_brh_lookup(brh_dir).filter(
    (pl.col('BRANCH') < 900)
    & (~pl.col('BRANCH').is_in([1, 99, 100, 218]))
)
print(f"Branches after <900 & !(1,99,100,218) filter: {brh_df.height}")

nobr    = brh_df.height
noacct  = loan_df.height
noacrel = round(noacct * 0.3 / nobr) if nobr > 0 else 0
print(f"Number of branches: {nobr}")
print(f"Number of accounts: {noacct:,}")
print(f"Average accounts per branch (noacrel): {noacrel}")

# ---------------------------------------------------------------------------
# Per-branch cap
# ---------------------------------------------------------------------------
loan11_df = (
    loan_df.group_by('BRABBR')
    .agg(pl.len().alias('NOACCT'))
    .with_columns(
        pl.col('NOACCT').mul(0.3).round().cast(pl.Int64).alias('AVGNO_temp')
    )
    .with_columns(
        pl.when(pl.col('AVGNO_temp') < noacrel)
          .then(pl.lit(noacrel)).otherwise(pl.col('AVGNO_temp')).alias('AVGNO')
    )
    .drop('AVGNO_temp')
)

loan1_df = loan_df.join(loan11_df, on='BRABBR', how='left')

# ---------------------------------------------------------------------------
# BRTXT1.txt
# ---------------------------------------------------------------------------
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
        output_lines.append(
            f"{row['ISSMTH']:02d}{row['ISSYR']}"
            f"{str(row.get('ACCTNO','')).ljust(10)[:10]}"
            f"{str(row.get('NAME','')).ljust(30)[:30]}"
            f"{row['ISSDT'].strftime('%d%m%Y')}"
            f"{str(row.get('BRABBR','')).ljust(3)[:3]}"
            f"{row.get('NEWSEC','')}"
        )

with open(output_path / "BRTXT1.txt", "w") as f:
    f.write("\n".join(output_lines))
print(f"\nGenerated BRTXT1.txt with {len(output_lines):,} records")

# ---------------------------------------------------------------------------
# Intermediate outputs
# ---------------------------------------------------------------------------
loan_df.write_csv(output_path / "LOAN.txt",   separator="|")
loan11_df.write_csv(output_path / "LOAN11.txt", separator="|")
loan1_df.write_csv(output_path / "LOAN1.txt",  separator="|")
print(f"Wrote LOAN.txt, LOAN11.txt, LOAN1.txt to {output_path}")

print("\nProcessing completed!")
