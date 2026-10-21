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

# *** TEST MODE: only read this many rows ***
TEST_ROW_LIMIT = 100_000

# ---------------------------------------------------------------------------
# HPD codes
# ---------------------------------------------------------------------------
_hpd_raw = os.environ.get("HPD_CODES", "").strip()
if _hpd_raw:
    HPD_VALUES = [int(v.strip()) for v in _hpd_raw.split(",") if v.strip()]
else:
    HPD_VALUES = list(PBBLNFMT.HPD)
print(f"HPD_VALUES: {HPD_VALUES}")

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
# ISSUEDT parser — handles real SAS date, Z11 numeric, string
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
# Read only first N rows
# ---------------------------------------------------------------------------
def read_first_n_rows(filepath: Path, n: int, cols: list) -> pl.DataFrame:
    if not filepath.exists():
        print(f"File not found: {filepath}")
        return pl.DataFrame()

    reader = pyreadstat.read_file_in_chunks(
        pyreadstat.read_sas7bdat, str(filepath),
        chunksize=min(n, 50_000), usecols=cols,
    )

    collected = []
    total = 0
    for pdf_chunk, _meta in reader:
        if total >= n:
            break
        take = min(len(pdf_chunk), n - total)
        collected.append(pl.from_pandas(pdf_chunk.iloc[:take]))
        total += take

    if not collected:
        return pl.DataFrame()
    return pl.concat(collected, how="diagonal")


# ---------------------------------------------------------------------------
# Filter funnel with full diagnostics
# ---------------------------------------------------------------------------
def filter_and_report(df: pl.DataFrame, reptmon: int, reptyear: int):
    df = parse_issuedt(df)

    ec   = pl.col('ENTITY_CD').cast(pl.Utf8).str.strip_chars()
    lt   = pl.col('LOANTYPE').cast(pl.Int64, strict=False)
    paid = pl.col('PAIDIND').cast(pl.Utf8).str.strip_chars().str.to_uppercase()
    bal  = pl.col('BALANCE').cast(pl.Float64, strict=False)
    mo   = pl.col('ISSMTH') == reptmon
    yr   = pl.col('ISSYR')  == reptyear

    n0 = df.height
    n1 = df.filter(ec != 'PIBB').height
    n2 = df.filter((ec != 'PIBB') & lt.is_in(HPD_VALUES)).height
    n3 = df.filter((ec != 'PIBB') & lt.is_in(HPD_VALUES) & (paid != 'P')).height
    n4 = df.filter((ec != 'PIBB') & lt.is_in(HPD_VALUES) & (paid != 'P') & (bal > 0)).height
    n5 = df.filter((ec != 'PIBB') & lt.is_in(HPD_VALUES) & (paid != 'P') & (bal > 0) & mo & yr).height

    print(f"\n--- FILTER FUNNEL (conventional) ---")
    print(f"  total rows:                              {n0:,}")
    print(f"  after ENTITY_CD != 'PIBB':               {n1:,}")
    print(f"  after LOANTYPE in HPD:                   {n2:,}")
    print(f"  after PAIDIND != 'P':                    {n3:,}")
    print(f"  after BALANCE > 0:                       {n4:,}")
    print(f"  after ISSMTH=={reptmon} & ISSYR=={reptyear}:           {n5:,}")

    print("\n--- Column stats ---")
    print(f"LOANTYPE dtype: {df['LOANTYPE'].dtype}")
    print(f"PAIDIND counts: {df.group_by('PAIDIND').len().sort('len', descending=True).to_dicts()}")
    print(f"ENTITY_CD counts: {df.group_by('ENTITY_CD').len().sort('len', descending=True).to_dicts()}")
    print(f"ISSMTH dist: {df.group_by('ISSMTH').len().sort('ISSMTH').to_dicts()}")
    print(f"ISSYR dist (top years): "
          f"{df.group_by('ISSYR').len().sort('len', descending=True).head(10).to_dicts()}")

    print("\n--- HPD loans that pass PAID+BAL filters, by year-month (top 20) ---")
    print(
        df.filter(lt.is_in(HPD_VALUES) & (paid != 'P') & (bal > 0))
          .group_by(['ISSYR', 'ISSMTH']).len()
          .sort(['ISSYR', 'ISSMTH'], descending=True)
          .head(20).to_dicts()
    )

    print("\n--- HPD loans by year-month (ignoring PAIDIND/BALANCE), top 20 ---")
    print(
        df.filter(lt.is_in(HPD_VALUES))
          .group_by(['ISSYR', 'ISSMTH']).len()
          .sort(['ISSYR', 'ISSMTH'], descending=True)
          .head(20).to_dicts()
    )

    # Islamic funnel
    ni0 = df.height
    ni1 = df.filter(ec == 'PIBB').height
    ni2 = df.filter((ec == 'PIBB') & lt.is_in(HPD_VALUES)).height
    ni3 = df.filter((ec == 'PIBB') & lt.is_in(HPD_VALUES) & (paid != 'P')).height
    ni4 = df.filter((ec == 'PIBB') & lt.is_in(HPD_VALUES) & (paid != 'P') & (bal > 0)).height
    ni5 = df.filter((ec == 'PIBB') & lt.is_in(HPD_VALUES) & (paid != 'P') & (bal > 0) & mo & yr).height

    print(f"\n--- FILTER FUNNEL (islamic) ---")
    print(f"  total rows:                              {ni0:,}")
    print(f"  after ENTITY_CD == 'PIBB':               {ni1:,}")
    print(f"  after LOANTYPE in HPD:                   {ni2:,}")
    print(f"  after PAIDIND != 'P':                    {ni3:,}")
    print(f"  after BALANCE > 0:                       {ni4:,}")
    print(f"  after ISSMTH=={reptmon} & ISSYR=={reptyear}:           {ni5:,}")

    conv = df.filter(
        (ec != 'PIBB') & lt.is_in(HPD_VALUES) & (paid != 'P')
        & (bal > 0) & mo & yr
    )
    islm = df.filter(
        (ec == 'PIBB') & lt.is_in(HPD_VALUES) & (paid != 'P')
        & (bal > 0) & mo & yr
    )
    return conv, islm


# ---------------------------------------------------------------------------
# Branch lookup
# ---------------------------------------------------------------------------
def _resolve_lkp_branch_path(brh_dir: Path) -> Optional[Path]:
    for c in [
        brh_dir / "LKP_BRANCH",
        brh_dir / "LKP_BRANCH.sas7bdat",
        brh_dir / "LKP_BRANCH" / "lkp_branch.sas7bdat",
        brh_dir / "LKP_BRANCH" / "LKP_BRANCH.sas7bdat",
        brh_dir / "lkp_branch.sas7bdat",
    ]:
        if c.exists() and c.is_file():
            return c
    return None


def read_brh_lookup(brh_dir: Path) -> pl.DataFrame:
    lkp = _resolve_lkp_branch_path(brh_dir)
    if lkp is not None:
        try:
            brh_df, _ = pyreadstat.read_sas7bdat(str(lkp))
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
            print(f"Loaded branch lookup from {lkp}: {brh.height} rows")
            return brh
        except Exception as exc:
            print(f"WARNING: failed to read {lkp} ({exc}); using BRCHCD_MAP")

    brh = pl.DataFrame([{"BRANCH": k, "BRABBR": v}
                        for k, v in PBBELF.BRCHCD_MAP.items()])
    print(f"Using PBBELF.BRCHCD_MAP: {brh.height} rows")
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

print(f"\nReading up to {TEST_ROW_LIMIT:,} rows from: {lnnote_file}")

raw = read_first_n_rows(lnnote_file, TEST_ROW_LIMIT, LNNOTE_COLUMNS)
if raw.is_empty():
    print("ERROR: could not read any rows")
    sys.exit(1)
print(f"Loaded {raw.height:,} rows")

conv_df, islm_df = filter_and_report(raw, reptmon_int, reptyear_int)

lnnote_combined = pl.concat([conv_df, islm_df], how="diagonal")
print(f"\nCombined filtered rows: {lnnote_combined.height:,}")

if lnnote_combined.is_empty():
    print("\n*** No data after filtering — see the funnel above for which step killed it. ***")
    print("Try different months:")
    print("  REPTMON=08 REPTYEAR=2026 python EIBMBRAS.py")
    print("  REPTMON=07 REPTYEAR=2026 python EIBMBRAS.py")
    print("\nOr check which month has HPD loans by looking at the")
    print("'HPD loans by year-month' sections above.")
    sys.exit(0)

# ---------------------------------------------------------------------------
# Build LOAN / LOAN11 / LOAN1 and BRTXT1
# ---------------------------------------------------------------------------
loan_df = lnnote_combined.with_columns([
    pl.when(pl.col('COLLDESC').cast(pl.Utf8).str.slice(37, 1).is_in(['N', 'R']))
      .then(pl.lit('N')).otherwise(pl.lit('S')).alias('NEWSEC'),
    pl.col('NTBRCH').cast(pl.Int64, strict=False)
      .map_elements(PBBELF.format_brchcd, return_dtype=pl.Utf8).alias('BRABBR'),
])

brh_df = read_brh_lookup(brh_dir).filter(
    (pl.col('BRANCH') < 900)
    & (~pl.col('BRANCH').is_in([1, 99, 100, 218]))
)

nobr    = brh_df.height
noacct  = loan_df.height
noacrel = round(noacct * 0.3 / nobr) if nobr > 0 else 0
print(f"\nBranches: {nobr}")
print(f"Accounts: {noacct}")
print(f"Avg per branch (30%): {noacrel}")

loan11_df = (
    loan_df.group_by('BRABBR')
    .agg(pl.len().alias('NOACCT'))
    .with_columns(pl.col('NOACCT').mul(0.3).round().cast(pl.Int64).alias('AVGNO_temp'))
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
print(f"\nGenerated BRTXT1.txt with {len(output_lines)} records")

loan_df.write_csv(output_path / "LOAN.txt",   separator="|")
loan11_df.write_csv(output_path / "LOAN11.txt", separator="|")
loan1_df.write_csv(output_path / "LOAN1.txt",  separator="|")

print("\nProcessing completed! (TEST MODE: only 100,000 rows scanned)")
