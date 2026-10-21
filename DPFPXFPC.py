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
input_pbb_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS")
input_pibb_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS")
brh_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIHPTOP")
output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMBRAS")
output_path.mkdir(parents=True, exist_ok=True)

# Only pull the columns we actually use. ENTITY_CD is required for the
# PIBB/conventional split, so it must be included.
LNNOTE_COLUMNS = [
    "ISSUEDT", "LOANTYPE", "PAIDIND", "BALANCE", "NTBRCH",
    "COLLDESC", "ACCTNO", "NAME", "ENTITY_CD",
]

CHUNKSIZE = 500_000

# ---------------------------------------------------------------------------
# HPD codes
# In the original SAS, &HPD was a macro variable resolved at compile time
# from PBBLNFMT. The macro constant HPD = [128, 130, 131, 132, 380, 381,
# 700, 705, 720, 725]. Note PBBLNFMT also defines a broader HP list; the
# original driver used &HPD (not &HP) in this DATA step, so we use HPD.
#
# We allow an env-var override so callers can reproduce whatever SAS passed
# in without editing this file, but fall back to the documented PBBLNFMT.HPD.
# ---------------------------------------------------------------------------
_hpd_raw = os.environ.get("HPD_CODES", "").strip()
if _hpd_raw:
    HPD_VALUES = [int(v.strip()) for v in _hpd_raw.split(",") if v.strip()]
    print(f"HPD_CODES overridden from environment: {HPD_VALUES}")
else:
    HPD_VALUES = list(PBBLNFMT.HPD)
    print(f"Using HPD from PBBLNFMT.HPD: {HPD_VALUES}")

# ---------------------------------------------------------------------------
# Reporting date parameters (same day-of-month bucketing as SAS REPTDATE)
# ---------------------------------------------------------------------------
reptdate = datetime.datetime.now() - datetime.timedelta(days=20)
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
def filter_lnnote_chunk(pdf, reptmon: int, reptyear: int, entity_filter: str) -> pl.DataFrame:
    """Convert one pandas chunk to polars, apply ENTITY_CD split and the
    same LOANTYPE/PAIDIND/BALANCE/ISSUE-month filters as the SAS step.

    entity_filter is one of:
        "conventional" -> ENTITY_CD != 'PIBB'
        "islamic"      -> ENTITY_CD =  'PIBB'
    """
    if pdf.empty:
        return pl.DataFrame()

    df = pl.from_pandas(pdf)

    # ISSUEDT encoding: MDDYYYYDDD (10 digits) or MMDDYYYYDDD (11 digits).
    # Left-pad to 11 then read MM/DD/YYYY at fixed positions.
    df = df.with_columns(
        pl.col('ISSUEDT').cast(pl.Int64).cast(pl.Utf8).alias('_raw')
    ).with_columns(
        pl.col('_raw').str.pad_start(11, '0').alias('_padded')
    ).with_columns([
        pl.col('_padded').str.slice(0, 2).cast(pl.Int32, strict=False).alias('ISSMTH'),
        pl.col('_padded').str.slice(2, 2).cast(pl.Int32, strict=False).alias('ISSDAY'),
        pl.col('_padded').str.slice(4, 4).cast(pl.Int32, strict=False).alias('ISSYR'),
    ]).with_columns(
        pl.date(pl.col('ISSYR'), pl.col('ISSMTH'), pl.col('ISSDAY')).alias('ISSDT')
    ).drop(['_raw', '_padded', 'ISSDAY'])

    # ENTITY_CD filter (SAS WHERE clause applied before the DATA step logic)
    if entity_filter == "conventional":
        df = df.filter(pl.col('ENTITY_CD') != 'PIBB')
    elif entity_filter == "islamic":
        df = df.filter(pl.col('ENTITY_CD') == 'PIBB')
    else:
        raise ValueError(f"Unknown entity_filter: {entity_filter}")

    df = df.filter(
        pl.col('LOANTYPE').cast(pl.Int64).is_in(HPD_VALUES)
        & (pl.col('PAIDIND') != 'P')
        & (pl.col('BALANCE') > 0)
        & (pl.col('ISSMTH') == reptmon)
        & (pl.col('ISSYR') == reptyear)
    )
    return df


def read_lnnote(filepath: Path, reptmon: int, reptyear: int,
                entity_filter: str, chunksize: int = CHUNKSIZE) -> pl.DataFrame:
    """Stream a .sas7bdat file in chunks, filtering each chunk immediately
    so the full file never sits in memory."""
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
    for pdf_chunk, _meta in reader:
        total_rows += len(pdf_chunk)
        filtered = filter_lnnote_chunk(pdf_chunk, reptmon, reptyear, entity_filter)
        if filtered.height > 0:
            filtered_chunks.append(filtered)

    kept = sum(c.height for c in filtered_chunks)
    print(f"{filepath.name} [{entity_filter}]: scanned {total_rows:,} rows, "
          f"kept {kept:,} after filter")

    if filtered_chunks:
        return pl.concat(filtered_chunks, how="diagonal")
    return pl.DataFrame()


def read_brh_lookup(brh_path: Path) -> pl.DataFrame:
    """Read LKP_BRANCH and reduce to the BRANCH / BRABBR pairs used in the
    original BRHDATA step. If the lookup file is not present, fall back to
    PBBELF.BRCHCD_MAP (same content the SAS BRHFILE provided)."""
    if brh_path.exists():
        try:
            brh_df, _ = pyreadstat.read_sas7bdat(str(brh_path))
            brh = pl.from_pandas(brh_df)
            # Normalise column names (SAS often stores them upper case)
            brh = brh.rename({c: c.upper() for c in brh.columns})
            # Accept either BRABBR or BRCHNAME style names
            if 'BRABBR' not in brh.columns:
                for cand in ('BRCHNAME', 'BRANCH_NAME', 'BRNM'):
                    if cand in brh.columns:
                        brh = brh.rename({cand: 'BRABBR'})
                        break
            brh = brh.select([
                pl.col('BRANCH').cast(pl.Int64),
                pl.col('BRABBR').cast(pl.Utf8),
            ])
            print(f"Loaded branch lookup from {brh_path}: {brh.height} rows")
            return brh
        except Exception as exc:
            print(f"WARNING: failed to read {brh_path} ({exc}); "
                  f"falling back to PBBELF.BRCHCD_MAP")

    brh_data = [{"BRANCH": k, "BRABBR": v} for k, v in PBBELF.BRCHCD_MAP.items()]
    brh = pl.DataFrame(brh_data)
    print(f"Using PBBELF.BRCHCD_MAP as branch lookup: {brh.height} rows")
    return brh


# ---------------------------------------------------------------------------
# *** A/C RELEASED FOR THE MTH ***
# Read + filter both LNNOTE sources.
#   pbb  -> conventional only (ENTITY_CD != 'PIBB')
#   pibb -> islamic only      (ENTITY_CD == 'PIBB')
# ---------------------------------------------------------------------------
reptmon_int = int(global_vars['REPTMON'])
reptyear_int = int(global_vars['REPTYEAR'])

lnnote_pbb = read_lnnote(input_pbb_path / "enrh_ln_note_m{reptmon}.sas7bdat",
                         reptmon_int, reptyear_int, "conventional")
lnnote_pibb = read_lnnote(input_pibb_path / "enrh_ln_note_m{reptmon}.sas7bdat",
                          reptmon_int, reptyear_int, "islamic")

lnnote_combined = pl.concat([lnnote_pbb, lnnote_pibb], how="diagonal")

if not lnnote_combined.is_empty():
    loan_df = lnnote_combined.with_columns([
        # NEWSEC: SUBSTR(COLLDESC,38,1) IN ('N','R')  -> 'N', else 'S'
        # SAS SUBSTR is 1-based, so position 38 = python index 37.
        pl.when(pl.col('COLLDESC').str.slice(37, 1).is_in(['N', 'R']))
        .then(pl.lit('N'))
        .otherwise(pl.lit('S'))
        .alias('NEWSEC'),
        # BRABBR: PUT(NTBRCH, BRCHCD.) via PBBELF.format_brchcd
        pl.col('NTBRCH').cast(pl.Int64)
        .map_elements(PBBELF.format_brchcd, return_dtype=pl.Utf8)
        .alias('BRABBR'),
    ])

    # *** AVERAGE PER BR BASE ON 30% OF TOTAL ***
    brh_df = read_brh_lookup(brh_path / "LKP_BRANCH").filter(
        (pl.col('BRANCH') < 900)
        & (~pl.col('BRANCH').is_in([1, 99, 100, 218]))
    )

    nobr = brh_df.height
    noacct = loan_df.height
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
            issyr = f"{row['ISSYR']}"
            acctno = str(row.get('ACCTNO', '')).ljust(10)[:10]
            name = str(row.get('NAME', '')).ljust(30)[:30]
            issdt = row['ISSDT'].strftime('%d%m%Y')
            brabb = str(row.get('BRABBR', '')).ljust(3)[:3]
            newsec = str(row.get('NEWSEC', ''))

            output_lines.append(
                f"{issmth}{issyr}{acctno}{name}{issdt}{brabb}{newsec}"
            )

    with open(output_path / "BRTXT1.txt", "w") as f:
        f.write("\n".join(output_lines))
    print(f"Generated BRTXT1.txt with {len(output_lines)} records")

    # Intermediate outputs for debugging
    loan_df.write_csv(output_path / "LOAN.txt", separator="|")
    loan11_df.write_csv(output_path / "LOAN11.txt", separator="|")
    loan1_df.write_csv(output_path / "LOAN1.txt", separator="|")

else:
    print("No LNNOTE data found after filtering")

print("Processing completed!")
