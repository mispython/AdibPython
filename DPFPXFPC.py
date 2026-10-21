import polars as pl
import pyreadstat
from pathlib import Path
import os

LNNOTE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/a_lnnote")
COLS = ["ISSUEDT", "LOANTYPE", "PAIDIND", "BALANCE", "NTBRCH",
        "COLLDESC", "ACCTNO", "NAME", "ENTITY_CD"]

# ---- Find the file for the month we want ----
REPTMON = os.environ.get("REPTMON", "09")
lnnote_file = LNNOTE_DIR / f"enrh_ln_note_m{REPTMON}.sas7bdat"
if not lnnote_file.exists():
    print(f"File not found: {lnnote_file}")
    print(f"Files available in {LNNOTE_DIR}:")
    for p in sorted(LNNOTE_DIR.glob("*.sas7bdat")):
        size_mb = p.stat().st_size / 1e6
        print(f"  {p.name}  ({size_mb:.0f} MB)")
    raise SystemExit(1)

print(f"Scanning: {lnnote_file}")
print("(streaming in 1M-row chunks, applying only cheap filters)\n")

# ---- Parse ISSUEDT ----
def parse_issuedt(df):
    src = df['ISSUEDT']
    if src.dtype in (pl.Date, pl.Datetime):
        return df.with_columns(
            src.cast(pl.Date).alias('ISSDT')
        ).with_columns(
            pl.col('ISSDT').dt.month().cast(pl.Int32).alias('ISSMTH'),
            pl.col('ISSDT').dt.year().cast(pl.Int32).alias('ISSYR'),
        )
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

# ---- Scan the whole file, keep only ACTIVE rows (paid != P, bal > 0) ----
active_chunks = []
total_rows = 0

reader = pyreadstat.read_file_in_chunks(
    pyreadstat.read_sas7bdat, str(lnnote_file),
    chunksize=1_000_000, usecols=COLS,
)

for i, (pdf, _meta) in enumerate(reader):
    df = pl.from_pandas(pdf)
    total_rows += df.height

    if i == 0:
        # Show first-chunk sample of raw ISSUEDT
        print(f"[chunk 0] ISSUEDT dtype: {df['ISSUEDT'].dtype}")
        print(f"[chunk 0] ISSUEDT sample: {df['ISSUEDT'].head(5).to_list()}")
        print(f"[chunk 0] LOANTYPE dtype: {df['LOANTYPE'].dtype}")
        print(f"[chunk 0] ENTITY_CD sample: {df['ENTITY_CD'].head(5).to_list()}\n")

    df = parse_issuedt(df)

    active = df.filter(
        (pl.col('PAIDIND').cast(pl.Utf8).str.strip_chars().str.to_uppercase() != 'P')
        & (pl.col('BALANCE').cast(pl.Float64, strict=False) > 0)
    ).select(['LOANTYPE', 'NTBRCH', 'ISSMTH', 'ISSYR', 'ENTITY_CD', 'COLLDESC'])

    if active.height > 0:
        active_chunks.append(active)

    if (i + 1) % 4 == 0:
        print(f"  scanned {total_rows:,} rows ...")

print(f"\nTotal rows scanned: {total_rows:,}")

if not active_chunks:
    print("No active rows at all (paid != P AND balance > 0). Nothing to analyze.")
    raise SystemExit(0)

active = pl.concat(active_chunks, how="diagonal")
print(f"Active rows (paid != P & bal > 0): {active.height:,}\n")

# ============================================================
# ANSWER 1: What LOANTYPE codes actually exist in the active book?
# This is what you'd replace &HPD with.
# ============================================================
print("=" * 70)
print("TOP 40 LOANTYPE in ACTIVE book (paid!=P, bal>0), by row count")
print("=" * 70)
top_lt = (
    active.group_by('LOANTYPE').len()
          .sort('len', descending=True)
          .head(40)
)
for row in top_lt.iter_rows(named=True):
    print(f"  LOANTYPE {int(row['LOANTYPE']) if row['LOANTYPE'] is not None else '?'}: {row['len']:,}")

# ============================================================
# ANSWER 2: Which months have data for each candidate list?
# This is what the SAS proc sql on MNILN.LNNOTE would tell you.
# ============================================================
print("\n" + "=" * 70)
print("YEAR-MONTH distribution of ACTIVE rows (top 30)")
print("=" * 70)
ym = (
    active.group_by(['ISSYR', 'ISSMTH']).len()
          .sort(['ISSYR', 'ISSMTH'], descending=True)
          .head(30)
)
for row in ym.iter_rows(named=True):
    print(f"  {row['ISSYR']}-{row['ISSMTH']:02d}: {row['len']:,}")

# ============================================================
# ANSWER 3: For each candidate HPD list, count active rows per year-month
# This directly answers "does &HPD even matter, or is the whole file empty?"
# ============================================================
candidate_lists = {
    "PBBLNFMT.HPD (10 codes)":  [128, 130, 131, 132, 380, 381, 700, 705, 720, 725],
    "PBBLNFMT.HP (17 codes)":   [128, 130, 131, 132, 380, 381, 700, 705, 720, 725,
                                 983, 993, 996, 678, 679, 698, 699],
    "LNFMT.LSTAFF (mass retail)": [4, 5, 6, 7, 15, 20, 25, 26, 27, 28, 29, 30, 31,
                                    32, 33, 34, 60, 61, 62, 63, 70, 71, 72, 73, 74,
                                    75, 76, 77, 78, 100],
    "LNFMT.LHOUSE (housing)":   [110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
                                 124, 139, 140, 141, 142, 145, 147, 173, 200, 201,
                                 204, 205, 209, 210, 211, 212, 213, 214, 215, 216,
                                 217, 218, 219, 220, 225, 226, 227, 228, 230, 231,
                                 232, 233, 234, 235, 236, 237, 238, 239, 240, 241,
                                 242, 243, 244, 245, 246, 247, 248],
    "LNFMT.LHPPRD (HP product)": [128, 130, 380, 381, 700, 705, 720, 725, 983, 993],
}

print("\n" + "=" * 70)
print("ACTIVE rows per candidate HPD list (all months combined)")
print("=" * 70)
lt_i64 = pl.col('LOANTYPE').cast(pl.Int64, strict=False)
for name, codes in candidate_lists.items():
    n = active.filter(lt_i64.is_in(codes)).height
    print(f"  {name:30s}: {n:,} rows")

# ============================================================
# ANSWER 4: For each candidate list, what's the most-recent month with data?
# ============================================================
print("\n" + "=" * 70)
print("MOST-RECENT year-month with data, per candidate list")
print("=" * 70)
for name, codes in candidate_lists.items():
    sub = active.filter(lt_i64.is_in(codes))
    if sub.height == 0:
        print(f"  {name:30s}: (no active rows)")
        continue
    newest = (
        sub.group_by(['ISSYR', 'ISSMTH']).len()
           .sort(['ISSYR', 'ISSMTH'], descending=True)
           .head(3)
           .to_dicts()
    )
    summary = ", ".join(f"{r['ISSYR']}-{r['ISSMTH']:02d}({r['len']})" for r in newest)
    print(f"  {name:30s}: {summary}")

# ============================================================
# ANSWER 5: The critical answer — what's in the file for the reporting month?
# ============================================================
REPTYEAR = int(os.environ.get("REPTYEAR", "2026"))
reptmon_int = int(REPTMON)
print("\n" + "=" * 70)
print(f"REPORTING MONTH {REPTYEAR}-{reptmon_int:02d} — how many active rows per candidate list?")
print("=" * 70)
month_filter = (pl.col('ISSYR') == REPTYEAR) & (pl.col('ISSMTH') == reptmon_int)
for name, codes in candidate_lists.items():
    n = active.filter(lt_i64.is_in(codes) & month_filter).height
    print(f"  {name:30s}: {n:,} rows")

# ============================================================
# ANSWER 6: NTBRCH distribution for the top candidate list (sanity)
# ============================================================
print("\n" + "=" * 70)
print("ENTITY_CD distribution of ACTIVE rows")
print("=" * 70)
ec = active.group_by('ENTITY_CD').len().sort('len', descending=True).head(10)
for row in ec.iter_rows(named=True):
    print(f"  {row['ENTITY_CD']!r}: {row['len']:,}")

print("\nDone.")
