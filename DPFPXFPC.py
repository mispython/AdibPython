import polars as pl
import pyreadstat
from pathlib import Path
import datetime

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
input_pbb_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMBRAS/pbb")
input_pibb_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMBRAS/pibb")
output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMBRAS")
output_path.mkdir(parents=True, exist_ok=True)

# Only pull the columns we actually use - this alone cuts I/O dramatically
# on the 20GB pbb file (reading fewer columns means fewer bytes off disk).
LNNOTE_COLUMNS = ["ISSUEDT", "LOANTYPE", "PAIDIND", "BALANCE", "NTBRCH", "COLLDESC", "ACCTNO", "NAME"]

# Rows per chunk when streaming the large pbb sas7bdat file. Tune this
# based on available RAM - 500k rows x ~8 columns is a small, safe chunk.
CHUNKSIZE = 500_000

HPD_VALUES = ['HPD']  # Update with actual HPD values from your SAS macro


# ---------------------------------------------------------------------------
# Reporting date parameters
# Previously read from a REPTDATE dataset - now derived directly as
# "today minus 1 day", then run through the same day-of-month bucketing
# logic the original SAS/polars code used.
# ---------------------------------------------------------------------------
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

mm = month
mm1 = (12 if month == 1 else month - 1) if wk == '1' else month
sdate = datetime.datetime(year, month, sdd)

global_vars = {
    'NOWK': wk,
    'NOWK1': wk1,
    'REPTMON': f"{mm:02d}",
    'REPTMON1': f"{mm1:02d}",
    'REPTYEAR': str(year),
    'REPTDAY': f"{day:02d}",
    'RDATE': reptdate.strftime('%d%m%y'),
    'SDATE': sdate.strftime('%d%m%y'),
}

print("Global variables:", global_vars)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def filter_lnnote_chunk(pdf, reptmon: int, reptyear: int) -> pl.DataFrame:
    """Convert one pandas chunk (from pyreadstat) to polars and apply the
    same filtering logic as the original step, as early as possible so we
    only ever keep the small filtered subset in memory, not the raw chunk."""
    if pdf.empty:
        return pl.DataFrame()

    df = pl.from_pandas(pdf)

    # ISSUEDT may already come back as a date/datetime from pyreadstat
    # (if the sas7bdat column has a date format attached), or as a raw
    # numeric/string field - handle both.
    if df.schema.get('ISSUEDT') in (pl.Date, pl.Datetime):
        df = df.with_columns(pl.col('ISSUEDT').cast(pl.Date).alias('ISSDT'))
    else:
        df = df.with_columns(
            pl.col('ISSUEDT').cast(pl.Utf8).str.slice(0, 8)
            .str.strptime(pl.Date, '%m%d%Y').alias('ISSDT')
        )

    df = df.with_columns([
        pl.col('ISSDT').dt.month().alias('ISSMTH'),
        pl.col('ISSDT').dt.year().alias('ISSYR'),
    ]).filter(
        (pl.col('LOANTYPE').is_in(HPD_VALUES)) &
        (pl.col('PAIDIND') != 'P') &
        (pl.col('BALANCE') > 0) &
        (pl.col('ISSMTH') == reptmon) &
        (pl.col('ISSYR') == reptyear)
    )
    return df


def read_lnnote(filepath: Path, reptmon: int, reptyear: int, chunksize: int = CHUNKSIZE) -> pl.DataFrame:
    """Stream a .sas7bdat file in chunks via pyreadstat, filtering each
    chunk immediately. This means the full 6M-row / 20GB pbb file never
    sits in memory at once - only the (much smaller) filtered rows
    accumulate across chunks."""
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
    for pdf_chunk, meta in reader:
        total_rows += len(pdf_chunk)
        filtered = filter_lnnote_chunk(pdf_chunk, reptmon, reptyear)
        if filtered.height > 0:
            filtered_chunks.append(filtered)

    kept = sum(c.height for c in filtered_chunks)
    print(f"{filepath.name}: scanned {total_rows:,} rows, kept {kept:,} after filter")

    if filtered_chunks:
        return pl.concat(filtered_chunks, how="diagonal")
    return pl.DataFrame()


# ---------------------------------------------------------------------------
# *** A/C RELEASED FOR THE MTH ***
# Read + filter both LNNOTE sources (pbb is the big ~20GB / ~6M row file)
# ---------------------------------------------------------------------------
reptmon_int = int(global_vars['REPTMON'])
reptyear_int = int(global_vars['REPTYEAR'])

lnnote_pbb = read_lnnote(input_pbb_path / "LNNOTE.sas7bdat", reptmon_int, reptyear_int)
lnnote_pibb = read_lnnote(input_pibb_path / "LNNOTE.sas7bdat", reptmon_int, reptyear_int)

lnnote_combined = pl.concat([lnnote_pbb, lnnote_pibb], how="diagonal")

if not lnnote_combined.is_empty():
    loan_df = lnnote_combined.with_columns([
        pl.when(pl.col('COLLDESC').str.slice(37, 1).is_in(['N', 'R']))
        .then(pl.lit('N'))
        .otherwise(pl.lit('S'))
        .alias('NEWSEC'),
        pl.col('NTBRCH').cast(pl.Utf8).alias('BRABBR'),
    ])

    # *** AVERAGE PER BR BASE ON 30% OF TOTAL ***
    # Replace with your actual branch file (INFILE equivalent)
    brh_data = [
        {"BRANCH": 2, "BRABBR": "001"},
        {"BRANCH": 3, "BRABBR": "002"},
        # Add more branch data as needed from your actual branch file
    ]
    brh_df = pl.DataFrame(brh_data).filter(
        (pl.col('BRANCH') < 900) &
        (~pl.col('BRANCH').is_in([1, 99, 100, 218]))
    )

    nobr = brh_df.height
    noacct = loan_df.height
    noacrel = round(noacct * 0.3 / nobr) if nobr > 0 else 0
    print(f"Number of branches: {nobr}")
    print(f"Number of accounts: {noacct}")
    print(f"Average accounts per branch: {noacrel}")

    # *** COMPARE RELEASE PER BR AND AVERAGE PER BR ***
    loan11_df = loan_df.group_by('BRABBR').agg([
        pl.len().alias('NOACCT')
    ]).with_columns([
        pl.col('NOACCT').mul(0.3).round().cast(pl.Int64).alias('AVGNO_temp')
    ]).with_columns([
        pl.when(pl.col('AVGNO_temp') < noacrel)
        .then(pl.lit(noacrel))
        .otherwise(pl.col('AVGNO_temp'))
        .alias('AVGNO')
    ]).drop('AVGNO_temp')

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
            issyr = f"{row['ISSYR']}"
            acctno = str(row.get('ACCTNO', '')).ljust(10)[:10]
            name = str(row.get('NAME', '')).ljust(30)[:30]
            issdt = row['ISSDT'].strftime('%d%m%Y')
            brabb = str(row.get('BRABBR', '')).ljust(3)[:3]
            newsec = str(row.get('NEWSEC', ''))

            line = f"{issmth}{issyr}{acctno}{name}{issdt}{brabb}{newsec}"
            output_lines.append(line)

    with open(output_path / "BRTXT1.txt", "w") as f:
        f.write("\n".join(output_lines))
    print(f"Generated BRTXT1.txt with {len(output_lines)} records")

    # Intermediate outputs as plain pipe-delimited text files
    loan_df.write_csv(output_path / "LOAN.txt", separator="|")
    loan11_df.write_csv(output_path / "LOAN11.txt", separator="|")
    loan1_df.write_csv(output_path / "LOAN1.txt", separator="|")

else:
    print("No LNNOTE data found after filtering")

print("Processing completed!")
