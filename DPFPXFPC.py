"""
Diagnose ENTITY_CD / BORSTAT in enrh_ln_note.sas7bdat
Run this BEFORE the main EIIFTXT1 script.
"""
import polars as pl
import pyreadstat
from pathlib import Path
import time

PATH_LN_NOTE = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/enrh_ln_note.sas7bdat')

print(f"File: {PATH_LN_NOTE}")
print(f"Exists: {PATH_LN_NOTE.exists()}")
print(f"Size: {PATH_LN_NOTE.stat().st_size:,} bytes\n")

# ---------------------------------------------------------------------------
# 1. Metadata: column names, types, row count (no data loaded)
# ---------------------------------------------------------------------------
print("=" * 60)
print("1. METADATA")
print("=" * 60)

_, meta = pyreadstat.read_sas7bdat(str(PATH_LN_NOTE), metadataonly=True)
print(f"Total rows (from metadata): {meta.number_rows:,}")
print(f"Total columns: {meta.number_columns}")

for col in ('ENTITY_CD', 'BORSTAT', 'LOANTYPE', 'ACCTNO', 'NOTENO'):
    if col in meta.column_names_to_labels:
        idx = meta.column_names_to_labels  # dict col->label
    if col in meta.column_names:
        i = meta.column_names.index(col)
        print(f"  {col}: type={meta.original_variable_types.get(col)}, "
              f"label={meta.column_names_to_labels.get(col, '')!r}")

# ---------------------------------------------------------------------------
# 2. ENTITY_CD distribution — read ONLY that column (fast)
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("2. ENTITY_CD DISTRIBUTION (full file)")
print("=" * 60)

t0 = time.time()
df_ent, _ = pyreadstat.read_sas7bdat(str(PATH_LN_NOTE), usecols=['ENTITY_CD'])
df_ent = pl.from_pandas(df_ent)
print(f"Read {df_ent.height:,} rows in {time.time()-t0:.1f}s")

df_ent = df_ent.with_columns(
    pl.col('ENTITY_CD').cast(pl.Utf8).str.strip_chars().alias('ENTITY_CLEAN')
)

print("\nRaw ENTITY_CD value counts (top 30):")
for r in (df_ent.group_by('ENTITY_CD').agg(pl.len())
          .sort('len', descending=True).head(30).iter_rows(named=True)):
    print(f"  {repr(r['ENTITY_CD']):>20} : {r['len']:>12,}")

print("\nStripped ENTITY_CLEAN value counts (top 30):")
for r in (df_ent.group_by('ENTITY_CLEAN').agg(pl.len())
          .sort('len', descending=True).head(30).iter_rows(named=True)):
    print(f"  {repr(r['ENTITY_CLEAN']):>20} : {r['len']:>12,}")

# Explicit PIBB check
pibb_exact   = df_ent.filter(pl.col('ENTITY_CD') == 'PIBB').height
pibb_strip   = df_ent.filter(pl.col('ENTITY_CLEAN') == 'PIBB').height
pibb_contains = df_ent.filter(
    pl.col('ENTITY_CD').cast(pl.Utf8).str.contains('PIBB', literal=True)
).height
print(f"\nPIBB matches:")
print(f"  exact 'PIBB'          : {pibb_exact:,}")
print(f"  stripped == 'PIBB'    : {pibb_strip:,}")
print(f"  contains 'PIBB'       : {pibb_contains:,}")

# ---------------------------------------------------------------------------
# 3. BORSTAT distribution — full file and within PIBB
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("3. BORSTAT DISTRIBUTION")
print("=" * 60)

t0 = time.time()
df_bs, _ = pyreadstat.read_sas7bdat(str(PATH_LN_NOTE),
                                    usecols=['ENTITY_CD', 'BORSTAT', 'LOANTYPE'])
df_bs = pl.from_pandas(df_bs)
print(f"Read {df_bs.height:,} rows in {time.time()-t0:.1f}s")

df_bs = df_bs.with_columns([
    pl.col('ENTITY_CD').cast(pl.Utf8).str.strip_chars().alias('ENTITY_CLEAN'),
    pl.col('BORSTAT').cast(pl.Utf8).str.strip_chars().alias('BORSTAT_CLEAN'),
])

print("\nBORSTAT across WHOLE file (stripped):")
for r in (df_bs.group_by('BORSTAT_CLEAN').agg(pl.len())
          .sort('len', descending=True).iter_rows(named=True)):
    print(f"  {repr(r['BORSTAT_CLEAN']):>10} : {r['len']:>12,}")

df_pibb = df_bs.filter(pl.col('ENTITY_CLEAN') == 'PIBB')
print(f"\nRows with ENTITY_CD='PIBB': {df_pibb.height:,}")
print("  BORSTAT distribution within PIBB:")
for r in (df_pibb.group_by('BORSTAT_CLEAN').agg(pl.len())
          .sort('len', descending=True).iter_rows(named=True)):
    print(f"    {repr(r['BORSTAT_CLEAN']):>10} : {r['len']:>12,}")

print("\n  LOANTYPE distribution within PIBB (top 20):")
for r in (df_pibb.group_by('LOANTYPE').agg(pl.len())
          .sort('len', descending=True).head(20).iter_rows(named=True)):
    print(f"    {r['LOANTYPE']} : {r['len']:>12,}")

# PIBB + BORSTAT='A'
pibb_a = df_pibb.filter(pl.col('BORSTAT_CLEAN') == 'A').height
print(f"\n  PIBB & BORSTAT='A': {pibb_a:,}")

# PIBB + BORSTAT='A' + LOANTYPE not in excluded
excluded = [983, 993, 678, 679, 698, 699]
pibb_a_valid = df_pibb.filter(
    (pl.col('BORSTAT_CLEAN') == 'A') &
    (~pl.col('LOANTYPE').is_in(excluded))
).height
print(f"  PIBB & BORSTAT='A' & LOANTYPE not excluded: {pibb_a_valid:,}")

# ---------------------------------------------------------------------------
# 4. Is the first 500k rows representative? (TEST_MODE sanity check)
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("4. FIRST 500K ROWS vs FULL FILE")
print("=" * 60)

df_head, _ = pyreadstat.read_sas7bdat(
    str(PATH_LN_NOTE), usecols=['ENTITY_CD', 'BORSTAT'], row_limit=500_000)
df_head = pl.from_pandas(df_head).with_columns([
    pl.col('ENTITY_CD').cast(pl.Utf8).str.strip_chars().alias('ENTITY_CLEAN'),
    pl.col('BORSTAT').cast(pl.Utf8).str.strip_chars().alias('BORSTAT_CLEAN'),
])

head_pibb  = df_head.filter(pl.col('ENTITY_CLEAN') == 'PIBB').height
total_pibb = df_ent.filter(pl.col('ENTITY_CLEAN') == 'PIBB').height
print(f"PIBB in first 500k rows : {head_pibb:,}")
print(f"PIBB in whole file      : {total_pibb:,}")
if total_pibb > 0:
    pct = 100.0 * head_pibb / total_pibb
    print(f"Test-mode captures      : {pct:.3f}% of all PIBB rows")

print("\nENTITY_CD in first 500k (top 10):")
for r in (df_head.group_by('ENTITY_CLEAN').agg(pl.len())
          .sort('len', descending=True).head(10).iter_rows(named=True)):
    print(f"  {repr(r['ENTITY_CLEAN']):>20} : {r['len']:>12,}")

# ---------------------------------------------------------------------------
# 5. Are PIBB rows physically clustered at the end of the file?
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("5. PHYSICAL POSITION OF PIBB ROWS")
print("=" * 60)

# Read ENTITY_CD again with an index to find where PIBB sits physically
df_idx, _ = pyreadstat.read_sas7bdat(str(PATH_LN_NOTE), usecols=['ENTITY_CD'])
df_idx = pl.from_pandas(df_idx)
df_idx = df_idx.with_row_index('_row').with_columns(
    pl.col('ENTITY_CD').cast(pl.Utf8).str.strip_chars().alias('ENTITY_CLEAN'))

pibb_rows = df_idx.filter(pl.col('ENTITY_CLEAN') == 'PIBB')['_row']
if pibb_rows.len() > 0:
    print(f"PIBB row positions (physical):")
    print(f"  min : {pibb_rows.min():,}")
    print(f"  max : {pibb_rows.max():,}")
    print(f"  mean: {pibb_rows.mean():,.0f}")
    print(f"  total file rows: {df_idx.height:,}")
    print(f"  PIBB rows in first 500k: {pibb_rows.filter(pl.Series(pibb_rows) < 500_000).len() if pibb_rows.len() else 0:,}")
else:
    print("No PIBB rows found — check the ENTITY_CD constant.")
