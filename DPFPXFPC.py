"""
EIMRESHP - HP/Hire Purchase Loan Summary & Detail Report

LNNOTE read optimized using polars_readstat's scan_readstat():
- Column projection pushdown: only reads the columns actually needed,
  not all ~400 columns in the 9GB file
- Row filters (ENTITY_CD / LOANTYPE / BALANCE) applied right after read
"""

import os
import sys
from datetime import datetime, timedelta

import polars as pl
from polars_readstat import scan_readstat

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LOAN_DIR    = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/a_lnnote'
CCDTEMP_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMREPOS'
OUTPUT_DIR  = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMRESHP'

HP_PRODUCTS    = [128, 130, 380, 381, 700, 705]
EXCLUDE_ENTITY = 'PIBB'

MAKE_MAP = {
    '1': 'PROTON', '2': 'PERODUA', '3': 'TOYOTA', '4': 'NISSAN',
    '5': 'HONDA', '6': 'ISUZU', '7': 'DAIHATSU', '8': 'MITSUBISHI',
    '9': 'FORD', '10': 'MERCEDES BENZ', '11': 'VOLVO', '13': 'BMW',
}

STATE_MAP = {
    '1': 'JOHORE', '2': 'KEDAH', '3': 'KELANTAN', '4': 'MALACCA',
    '5': 'N.SEMBILAN', '6': 'PAHANG', '7': 'PENANG', '8': 'PERAK',
    '9': 'PERLIS', '10': 'SABAH', '11': 'SARAWAK', '12': 'SELANGOR',
    '13': 'TRENGGANU', '14': 'W.PERSEKUTUAN', '15': 'LABUAN',
}

BUCKETS = [
    ('LT3',    '<3MTHS',     None, 3),
    ('B3_6',   '3-6MTHS',    3,    6),
    ('B6_12',  '6-12MTHS',   6,    12),
    ('B12_24', '12-24MTHS',  12,   24),
    ('B24_36', '24-36MTHS',  24,   36),
    ('GT36',   '>36MTHS',    36,   None),
]

REPORT_DATE_FMT = '%d/%m/%Y'
DETAIL_FMT      = '%d/%m/%Y'

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def read_loantemp(path: str) -> pl.DataFrame:
    """LOANTEMP is small — pyreadstat is fine here."""
    import pyreadstat
    df, _ = pyreadstat.read_sas7bdat(path)
    return pl.from_pandas(df)


def sas_date_to_datetime(col: pl.Expr) -> pl.Expr:
    epoch = pl.lit(datetime(1960, 1, 1)).cast(pl.Datetime)
    return epoch + pl.duration(days=col.cast(pl.Int64))


def build_mtharr_expr() -> pl.Expr:
    day_bounds = [
        (729, None), (698, 23), (668, 22), (638, 21), (608, 20),
        (577, 19), (547, 18), (516, 17), (486, 16), (456, 15),
        (424, 14), (394, 13), (364, 12), (333, 11), (303, 10),
        (273, 9), (243, 8), (213, 7), (182, 6), (151, 5),
        (121, 4), (91, 3), (61, 2), (30, 1), (None, 0),
    ]
    expr = None
    for lower, value in day_bounds:
        if lower is None:
            cond = pl.lit(True)
        elif value is None:
            expr = pl.when(pl.col('DAYDIFF') > lower).then(
                (pl.col('DAYDIFF') / 365 * 12).cast(pl.Int32)
            )
            continue
        else:
            cond = pl.col('DAYDIFF') > lower
            expr = expr.when(cond).then(pl.lit(value))
    return expr.otherwise(pl.lit(0))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    print("EIMRESHP - HP Loan Summary & Detail Report")
    print("=" * 60)

    # 1. Report date
    reptdate = datetime.now() - timedelta(days=21)
    day = reptdate.day
    if   day <= 7:  sdd, wk, wk1 = 1,  '1', '4'
    elif day <= 14: sdd, wk, wk1 = 9,  '2', '1'
    elif day <= 21: sdd, wk, wk1 = 16, '3', '2'
    else:           sdd, wk, wk1 = 23, '4', '3'
    mm = reptdate.month
    reptyear, reptmon, reptday = reptdate.year, f'{mm:02d}', f'{day:02d}'
    print(f"\nReport Date: {reptdate.strftime(REPORT_DATE_FMT)}  |  Week: {wk}")

    # 2. LOANTEMP
    print("\nReading LOANTEMP ...")
    loantemp = read_loantemp(
        os.path.join(CCDTEMP_DIR, 'loantemp.sas7bdat')
    ).filter(
        pl.col('PRODUCT').is_in(HP_PRODUCTS) & (pl.col('BALANCE') > 0)
    )
    print(f"  LOANTEMP filtered: {len(loantemp):,}")

    # -----------------------------------------------------------------------
    # 3. LNNOTE — optimized read (the key change)
    # -----------------------------------------------------------------------
    lnnote_path = os.path.join(LOAN_DIR, f'enrh_ln_note_m{reptmon}.sas7bdat')
    print(f"\nScanning LNNOTE (lazy, projected): {lnnote_path}")

    # Column projection: only these columns are read; the other ~380
    # columns never get pulled off disk.
    LNNOTE_COLS = [
        'ACCTNO', 'NOTENO', 'LOANTYPE', 'NETPROC', 'APPVALUE',
        'NOTETERM', 'STATE', 'DEALERNO', 'SCORE2', 'ORGBAL',
        'CURBAL', 'PAYAMT', 'ISSUEDT', 'NTBRCH', 'ENTITY_CD', 'BALANCE',
    ]

    # Probe schema first to confirm ENTITY_CD exists
    lf_probe = scan_readstat(lnnote_path)
    available = lf_probe.collect_schema().names()
    has_entity = 'ENTITY_CD' in available
    if not has_entity:
        print("  WARNING: ENTITY_CD not found — conventional filter skipped")

    entity_ok = (
        pl.col('ENTITY_CD').cast(pl.Utf8, strict=False) != EXCLUDE_ENTITY
        if has_entity else pl.lit(True)
    )

    # Lazy scan + projection + filter, then collect
    lnnote = (
        scan_readstat(lnnote_path)
        .select(LNNOTE_COLS)                         # projection pushdown — the key speedup
        .filter(
            entity_ok
            & pl.col('LOANTYPE').is_in(HP_PRODUCTS)
            & (pl.col('BALANCE') > 0)
        )
        .rename({'NTBRCH': 'BRABBR'})
        .collect(engine='streaming')                 # streaming collect to cap memory
    )
    print(f"  LNNOTE filtered: {len(lnnote):,}")

    # 4. Merge
    print("\nMerging ...")
    loantemp_cols = [
        'ACCTNO', 'NOTENO', 'NAME', 'CENSUS', 'BORSTAT',
        'BALANCE', 'PRODUCT', 'DAYDIFF',
    ]
    merged = lnnote.join(
        loantemp.select(loantemp_cols),
        on=['ACCTNO', 'NOTENO'],
        how='inner',
    )
    print(f"  Merged HP Loans: {len(merged):,}")

    # 5. Derived fields
    print("\nDeriving fields ...")
    df = merged.with_columns([
        ((pl.col('ORGBAL') - pl.col('CURBAL')) / pl.col('PAYAMT')).alias('ISTLPD'),
        sas_date_to_datetime(pl.col('ISSUEDT')).alias('ISSDTE'),
        pl.col('SCORE2').cast(pl.Utf8, strict=False).str.slice(0, 1)
            .fill_null('N/A').alias('CRRISK'),
        pl.when(pl.col('APPVALUE') > 0)
          .then((pl.col('NETPROC') / pl.col('APPVALUE') * 100).round(1))
          .otherwise(0.0)
          .alias('MARGINF'),
        pl.col('CENSUS').cast(pl.Utf8, strict=False).str.zfill(7).alias('CENSUS9'),
    ])

    df = df.with_columns([
        pl.when(pl.col('MARGINF') < 70).then(pl.lit('E. <70%'))
          .when(pl.col('MARGINF') < 80).then(pl.lit('D. 70 TO <80%'))
          .when(pl.col('MARGINF') < 85).then(pl.lit('C. 80 TO <85%'))
          .when(pl.col('MARGINF') < 89).then(pl.lit('B. 85 TO <89%'))
          .otherwise(pl.lit('A. 89% & ABV'))
          .alias('MGINGRP'),

        pl.when(pl.col('NOTETERM') <= 36).then(pl.lit('A. <=3 YRS'))
          .when(pl.col('NOTETERM') <= 48).then(pl.lit('B. 4 YRS'))
          .when(pl.col('NOTETERM') <= 60).then(pl.lit('C. 5 YRS'))
          .when(pl.col('NOTETERM') <= 72).then(pl.lit('D. 6 YRS'))
          .when(pl.col('NOTETERM') <= 84).then(pl.lit('E. 7 YRS'))
          .when(pl.col('NOTETERM') <= 96).then(pl.lit('F. 8 YRS'))
          .otherwise(pl.lit('G. 9 YRS'))
          .alias('TERMGRP'),

        pl.col('STATE').cast(pl.Utf8, strict=False)
          .replace_strict(STATE_MAP, default='OTHERS').alias('STATENM'),

        pl.when(pl.col('STATE').cast(pl.Utf8, strict=False).is_in(['10', '11', '15']))
          .then(pl.lit('EAST MALAYSIA'))
          .otherwise(pl.lit('WEST MALAYSIA')).alias('NATIONAL'),

        pl.col('CENSUS9').str.slice(0, 2).str.strip_chars()
          .replace_strict(MAKE_MAP, default='OTHERS').alias('MAKE'),

        pl.when(pl.col('CENSUS9').str.slice(3, 1).is_in(['1', '2']))
          .then(pl.lit('NEW')).otherwise(pl.lit('SECONDHAND')).alias('NEWSEC'),

        pl.when(pl.col('NETPROC') <= 30_000).then(pl.lit('A. RM30K & BELOW'))
          .when(pl.col('NETPROC') <= 50_000).then(pl.lit('B. >RM30K TO 50K'))
          .when(pl.col('NETPROC') <= 100_000).then(pl.lit('C. >RM50K TO 100K'))
          .when(pl.col('NETPROC') <= 250_000).then(pl.lit('D. >RM100K TO 250K'))
          .otherwise(pl.lit('E. >RM250K')).alias('FINGRP'),

        pl.when(pl.col('DEALERNO') > 0)
          .then(pl.lit('DEALERS')).otherwise(pl.lit('NON DEALERS')).alias('SOURCE'),
    ])

    df = df.with_columns([
        pl.when(pl.col('MAKE').is_in(['PROTON', 'PERODUA']))
          .then(pl.lit('NATIONAL')).otherwise(pl.lit('NON NATIONAL')).alias('CARS'),

        pl.when((pl.col('MAKE') == 'OTHERS') & pl.col('PRODUCT').is_in([128, 700]))
          .then(pl.lit('SCHEDULE'))
          .when(pl.col('MAKE') == 'OTHERS').then(pl.lit('UNSCHEDULE'))
          .otherwise(pl.lit('')).alias('GOODS'),
    ])

    df = df.with_columns([
        build_mtharr_expr().alias('MTHARR'),
    ]).with_columns([
        pl.when(pl.col('BORSTAT') == 'F')
          .then(pl.lit(999, dtype=pl.Int32))
          .otherwise(pl.col('MTHARR')).alias('MTHARR'),
    ])

    print(f"  Processed: {len(df):,}")

    # 6. Account groups
    is_npl = (pl.col('MTHARR') >= 3) | pl.col('BORSTAT').is_in(['F', 'I', 'R'])
    is_restructured = pl.col('NOTENO') >= 98010

    groups = {
        'HPLOAN1 (All)':              df,
        'HPLOAN2 (NPL)':              df.filter(is_npl),
        'HPLOAN3 (Restructured)':     df.filter(is_restructured),
        'HPLOAN4 (Restructured NPL)': df.filter(is_restructured & is_npl),
    }
    for label, gdf in groups.items():
        print(f"  {label}: {len(gdf):,}")

    # 7. Summary reports
    # (Same as original: build_summary / write_summary / report_catalog
    #  loop — omitted here for brevity; see the full production file.)

    # 8. Detail report
    print("\nGenerating NPL detail ...")
    detail = groups['HPLOAN2 (NPL)'].select([
        'ACCTNO', 'NOTENO', 'NAME', 'BRABBR', 'PRODUCT', 'BORSTAT',
        'NETPROC', 'BALANCE', 'MTHARR', 'MARGINF', 'NOTETERM',
        'STATENM', 'MAKE', 'NEWSEC', 'SOURCE', 'SCORE2', 'ISTLPD', 'ISSDTE',
    ]).sort(['ACCTNO', 'NOTENO'])

    detail_pd = detail.to_pandas()
    detail_pd['ISSDTE'] = detail_pd['ISSDTE'].dt.strftime(DETAIL_FMT).fillna('')

    detail_file = os.path.join(OUTPUT_DIR, 'EIMRESHP_DETAIL_NPL.txt')
    with open(detail_file, 'w') as f:
        f.write("=" * 180 + "\n")
        f.write("EIMRESHP - HIRE PURCHASE NPL DETAIL REPORT\n")
        f.write(f"REPORT DATE: {reptdate.strftime(REPORT_DATE_FMT)}\n")
        f.write(f"TOTAL NPL ACCOUNTS: {len(detail):,}\n")
        f.write(f"TOTAL NPL BALANCE : RM {detail['BALANCE'].sum():,.2f}\n")
        f.write("=" * 180 + "\n\n")
        f.write(detail_pd.to_string(index=False, justify='left',
                float_format=lambda x: f'{x:,.2f}', na_rep=''))
        f.write("\n\n" + "=" * 180 + "\nEND OF REPORT\n")

    print(f"  {len(detail):,} NPL accounts written")


if __name__ == '__main__':
    main()
