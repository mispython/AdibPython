"""
EIMRESHP - HP/Hire Purchase Loan Summary & Detail Report (Production Ready)

Purpose:
- Generate summary reports for HP loans (Conv & Aitab) by various groupings
- Track NPL accounts (>=3 months in arrears or F/I/R status)
- Monitor restructured accounts (NOTENO >= 98010)
- Detail report for NPL accounts

HP Products: 128, 130, 380, 381, 700, 705

LNNOTE filter: ENTITY_CD != 'PIBB' (exclude conventional), plus HP product & BALANCE > 0

Report Categories:
1. Credit Risk Score (CRRISK)
2. Source of Business (Dealers vs Non-Dealers)
3. Margin of Finance (<70%, 70-80%, 80-85%, 85-89%, 89%+)
4. Loan Term (<=3yrs, 4yrs, 5yrs, 6yrs, 7yrs, 8yrs, 9yrs+)
5. Amount Financed (<=30K, 30-50K, 50-100K, 100-250K, >250K)
6. By State (14 states + Labuan, grouped East/West Malaysia)
7. By Make of Vehicle (13 makes, National vs Non-National)
8. Make = OTHERS (Schedule vs Unschedule)

4 Account Groups:
- HPLOAN1: All HP accounts
- HPLOAN2: NPL (>=3 months OR F/I/R status)
- HPLOAN3: Restructured (NOTENO >= 98010)
- HPLOAN4: Restructured NPL

Arrears Buckets (in report order):
- <3 months, 3-6 months, 6-12 months, 12-24 months, 24-36 months, >36 months
"""

import os
import sys
from datetime import datetime, timedelta

import polars as pl
import pyreadstat

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LOAN_DIR    = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/a_lnnote'
CCDTEMP_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMREPOS'
OUTPUT_DIR  = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMRESHP'

HP_PRODUCTS    = [128, 130, 380, 381, 700, 705]
EXCLUDE_ENTITY = 'PIBB'          # exclude conventional accounts from LNNOTE

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
def read_sas(path: str, label: str) -> pl.DataFrame:
    """Read a .sas7bdat file and convert to Polars; exit on failure."""
    try:
        df, _ = pyreadstat.read_sas7bdat(path)
        df = pl.from_pandas(df)
        print(f"  {label}: {len(df):,} rows loaded from {os.path.basename(path)}")
        return df
    except Exception as exc:
        print(f"  ERROR reading {label} ({path}): {exc}")
        sys.exit(1)


def sas_date_to_datetime(col: pl.Expr) -> pl.Expr:
    """Convert SAS date (days since 1960-01-01) to Polars Datetime."""
    epoch = pl.lit(datetime(1960, 1, 1)).cast(pl.Datetime)
    return epoch + pl.duration(days=col.cast(pl.Int64))


def build_mtharr_expr() -> pl.Expr:
    """Build the MTHARR month-in-arrears expression from DAYDIFF."""
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
        elif value is None:  # beyond 24 months → scale by 365
            expr = pl.when(pl.col('DAYDIFF') > lower).then(
                (pl.col('DAYDIFF') / 365 * 12).cast(pl.Int32)
            )
            continue
        else:
            cond = pl.col('DAYDIFF') > lower
            expr = expr.when(cond).then(pl.lit(value))
    return expr.otherwise(pl.lit(0))


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def main() -> None:
    print("EIMRESHP - HP Loan Summary & Detail Report")
    print("=" * 60)

    # -----------------------------------------------------------------------
    # 1. Report date / week
    # -----------------------------------------------------------------------
    reptdate = datetime.now() - timedelta(days=21)
    day = reptdate.day

    if   day <= 7:  sdd, wk, wk1 = 1,  '1', '4'
    elif day <= 14: sdd, wk, wk1 = 9,  '2', '1'
    elif day <= 21: sdd, wk, wk1 = 16, '3', '2'
    else:           sdd, wk, wk1 = 23, '4', '3'

    mm = reptdate.month
    mm1 = 12 if (wk == '1' and mm == 1) else (mm - 1 if wk == '1' else mm)
    reptyear, reptmon, reptday = reptdate.year, f'{mm:02d}', f'{day:02d}'

    print(f"\nReport Date: {reptdate.strftime(REPORT_DATE_FMT)}  |  Week: {wk}")
    print("=" * 60)

    # -----------------------------------------------------------------------
    # 2. Load source files
    # -----------------------------------------------------------------------
    print("\nReading LOANTEMP.sas7bdat ...")
    loantemp = read_sas(
        os.path.join(CCDTEMP_DIR, 'loantemp.sas7bdat'),
        'LOANTEMP',
    ).filter(
        pl.col('PRODUCT').is_in(HP_PRODUCTS) & (pl.col('BALANCE') > 0)
    )
    print(f"  LOANTEMP filtered HP records: {len(loantemp):,}")

    print("\nReading LNNOTE.sas7bdat ...")
    lnnote = read_sas(
        os.path.join(LOAN_DIR, f'enrh_ln_note_m{reptmon}.sas7bdat'),
        'LNNOTE',
    )

    # -----------------------------------------------------------------------
    # 2a. LNNOTE filter: HP products, BALANCE > 0, ENTITY_CD != 'PIBB'
    # -----------------------------------------------------------------------
    if 'ENTITY_CD' not in lnnote.columns:
        print(f"  WARNING: ENTITY_CD not present in LNNOTE — conventional "
              f"filter ({EXCLUDE_ENTITY}) will be skipped")
        entity_ok = pl.lit(True)
    else:
        entity_ok = pl.col('ENTITY_CD').cast(pl.Utf8, strict=False) != EXCLUDE_ENTITY

    lnnote = lnnote.filter(
        entity_ok
        & pl.col('LOANTYPE').is_in(HP_PRODUCTS)
        & (pl.col('BALANCE') > 0)
    ).select([
        'ACCTNO', 'NOTENO', 'LOANTYPE', 'NETPROC', 'APPVALUE',
        'NOTETERM', 'STATE', 'DEALERNO', 'SCORE2', 'ORGBAL',
        'CURBAL', 'PAYAMT', 'ISSUEDT', 'NTBRCH',
    ]).rename({'NTBRCH': 'BRABBR'})

    print(f"  LNNOTE filtered HP records "
          f"(ENTITY_CD != '{EXCLUDE_ENTITY}'): {len(lnnote):,}")

    # -----------------------------------------------------------------------
    # 3. Merge — keep only columns actually needed downstream
    # -----------------------------------------------------------------------
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
    print(f"  Merged HP Loans: {len(merged):,} accounts")

    # -----------------------------------------------------------------------
    # 4. Derived fields
    # -----------------------------------------------------------------------
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
          .replace_strict(STATE_MAP, default='OTHERS')
          .alias('STATENM'),

        pl.when(pl.col('STATE').cast(pl.Utf8, strict=False).is_in(['10', '11', '15']))
          .then(pl.lit('EAST MALAYSIA'))
          .otherwise(pl.lit('WEST MALAYSIA'))
          .alias('NATIONAL'),

        pl.col('CENSUS9').str.slice(0, 2).str.strip_chars()
          .replace_strict(MAKE_MAP, default='OTHERS')
          .alias('MAKE'),

        pl.when(pl.col('CENSUS9').str.slice(3, 1).is_in(['1', '2']))
          .then(pl.lit('NEW'))
          .otherwise(pl.lit('SECONDHAND'))
          .alias('NEWSEC'),

        pl.when(pl.col('NETPROC') <= 30_000).then(pl.lit('A. RM30K & BELOW'))
          .when(pl.col('NETPROC') <= 50_000).then(pl.lit('B. >RM30K TO 50K'))
          .when(pl.col('NETPROC') <= 100_000).then(pl.lit('C. >RM50K TO 100K'))
          .when(pl.col('NETPROC') <= 250_000).then(pl.lit('D. >RM100K TO 250K'))
          .otherwise(pl.lit('E. >RM250K'))
          .alias('FINGRP'),

        pl.when(pl.col('DEALERNO') > 0)
          .then(pl.lit('DEALERS'))
          .otherwise(pl.lit('NON DEALERS'))
          .alias('SOURCE'),
    ])

    df = df.with_columns([
        pl.when(pl.col('MAKE').is_in(['PROTON', 'PERODUA']))
          .then(pl.lit('NATIONAL'))
          .otherwise(pl.lit('NON NATIONAL'))
          .alias('CARS'),

        pl.when((pl.col('MAKE') == 'OTHERS') & pl.col('PRODUCT').is_in([128, 700]))
          .then(pl.lit('SCHEDULE'))
          .when(pl.col('MAKE') == 'OTHERS')
          .then(pl.lit('UNSCHEDULE'))
          .otherwise(pl.lit(''))
          .alias('GOODS'),
    ])

    df = df.with_columns([
        build_mtharr_expr().alias('MTHARR'),
    ]).with_columns([
        pl.when(pl.col('BORSTAT') == 'F')
          .then(pl.lit(999, dtype=pl.Int32))
          .otherwise(pl.col('MTHARR'))
          .alias('MTHARR'),
    ])

    print(f"  Processed: {len(df):,} HP loans")

    # -----------------------------------------------------------------------
    # 5. Account groups
    # -----------------------------------------------------------------------
    print("\nCreating account groups ...")
    is_npl         = (pl.col('MTHARR') >= 3) | pl.col('BORSTAT').is_in(['F', 'I', 'R'])
    is_restructured = pl.col('NOTENO') >= 98010

    groups = {
        'HPLOAN1 (All)':              df,
        'HPLOAN2 (NPL)':              df.filter(is_npl),
        'HPLOAN3 (Restructured)':     df.filter(is_restructured),
        'HPLOAN4 (Restructured NPL)': df.filter(is_restructured & is_npl),
    }
    for label, gdf in groups.items():
        print(f"  {label}: {len(gdf):,}")

    # -----------------------------------------------------------------------
    # 6. Report generation
    # -----------------------------------------------------------------------
    def build_summary(frame: pl.DataFrame, group_cols: list[str]) -> pl.DataFrame:
        bucketed = frame.with_columns([
            pl.when(pl.col('MTHARR') < 3).then(pl.lit('LT3'))
              .when(pl.col('MTHARR') < 6).then(pl.lit('B3_6'))
              .when(pl.col('MTHARR') < 12).then(pl.lit('B6_12'))
              .when(pl.col('MTHARR') < 24).then(pl.lit('B12_24'))
              .when(pl.col('MTHARR') < 36).then(pl.lit('B24_36'))
              .otherwise(pl.lit('GT36'))
              .alias('BUCKET_KEY'),
        ])

        agg = bucketed.group_by(group_cols + ['BUCKET_KEY']).agg([
            pl.len().alias('CNT'),
            pl.col('BALANCE').sum().alias('AMT'),
        ])

        pivoted = agg.pivot(
            values=['CNT', 'AMT'],
            index=group_cols,
            on='BUCKET_KEY',
        )

        # Backfill any missing bucket columns with zero
        for key, _label, _lo, _hi in BUCKETS:
            for suffix, dtype in [('CNT', pl.UInt32), ('AMT', pl.Float64)]:
                col = f'{suffix}_{key}'
                if col not in pivoted.columns:
                    pivoted = pivoted.with_columns(pl.lit(0, dtype=dtype).alias(col))

        rename = {}
        ordered = list(group_cols)
        for key, _label, _lo, _hi in BUCKETS:
            rename[f'CNT_{key}'] = f'{key}_CNT'
            rename[f'AMT_{key}'] = f'{key}_AMT'
            ordered += [f'{key}_CNT', f'{key}_AMT']

        pivoted = pivoted.rename(rename).select(ordered)

        cnt_cols = [c for c in ordered if c.endswith('_CNT')]
        amt_cols = [c for c in ordered if c.endswith('_AMT')]
        pivoted = pivoted.with_columns([
            pl.sum_horizontal(*[pl.col(c) for c in cnt_cols]).alias('TOTAL_CNT'),
            pl.sum_horizontal(*[pl.col(c) for c in amt_cols]).alias('TOTAL_AMT'),
        ])
        return pivoted.sort(group_cols)

    def write_summary(
        summary: pl.DataFrame,
        group_cols: list[str],
        title: str,
        subtitle: str,
        filepath: str,
    ) -> None:
        display_cols = (group_cols +
                        [f'{k}_{s}' for k, _, _, _ in BUCKETS for s in ('CNT', 'AMT')] +
                        ['TOTAL_CNT', 'TOTAL_AMT'])

        pdf = summary.select(display_cols).to_pandas()

        col_map = {f'{k}_CNT': f'{lbl}_CNT' for k, lbl, _, _ in BUCKETS}
        col_map.update({f'{k}_AMT': f'{lbl}_AMT' for k, lbl, _, _ in BUCKETS})
        col_map['TOTAL_CNT'] = 'TOTAL_CNT'
        col_map['TOTAL_AMT'] = 'TOTAL_AMT'
        pdf = pdf.rename(columns=col_map)

        cnt_cols = [c for c in pdf.columns if c.endswith('_CNT')]
        amt_cols = [c for c in pdf.columns if c.endswith('_AMT')]
        totals = {c: pdf[c].sum() for c in cnt_cols + amt_cols}
        if group_cols:
            totals[group_cols[0]] = 'TOTAL'
        import pandas as pd
        pdf = pd.concat([pdf, pd.DataFrame([totals])], ignore_index=True)

        with open(filepath, 'w') as f:
            f.write("=" * 160 + "\n")
            f.write("EIMRESHP - HIRE PURCHASE LOAN SUMMARY REPORT\n")
            f.write(f"REPORT TITLE: {title}\n")
            f.write(f"SUB TITLE:   {subtitle}\n")
            f.write(f"REPORT DATE: {reptdate.strftime(REPORT_DATE_FMT)}\n")
            f.write(f"FILTER     : HP products {HP_PRODUCTS}; "
                    f"LNNOTE ENTITY_CD != '{EXCLUDE_ENTITY}'\n")
            f.write("=" * 160 + "\n\n")
            f.write(pdf.to_string(index=False, float_format=lambda x: f'{x:,.2f}'))
            f.write("\n\n")
            f.write("=" * 160 + "\n")
            f.write(f"GRAND TOTAL ACCOUNTS: {int(totals.get('TOTAL_CNT', 0)):,}\n")
            f.write(f"GRAND TOTAL AMOUNT  : RM {totals.get('TOTAL_AMT', 0.0):,.2f}\n")
            f.write("END OF REPORT\n")

    # Report catalogue: (frame_key, group columns, title, subtitle)
    report_catalog = [
        ('HPLOAN1 (All)',             ['CRRISK', 'BRABBR'],                'CREDIT RISK SCORE', 'PRODUCT 128,130,380,381,700,705'),
        ('HPLOAN2 (NPL)',             ['CRRISK', 'BRABBR'],                'CREDIT RISK SCORE', 'NPL ACCOUNT'),
        ('HPLOAN3 (Restructured)',    ['CRRISK', 'BRABBR'],                'CREDIT RISK SCORE', 'RESTRUCTURE ACCOUNT'),
        ('HPLOAN4 (Restructured NPL)',['CRRISK', 'BRABBR'],                'CREDIT RISK SCORE', 'RESTRUCTURE NPL ACCOUNT'),
        ('HPLOAN1 (All)',             ['SOURCE', 'BRABBR'],                'SOURCE OF BUSINESS', 'PRODUCT 128,130,380,381,700,705'),
        ('HPLOAN2 (NPL)',             ['SOURCE', 'BRABBR'],                'SOURCE OF BUSINESS', 'NPL ACCOUNT'),
        ('HPLOAN3 (Restructured)',    ['SOURCE', 'BRABBR'],                'SOURCE OF BUSINESS', 'RESTRUCTURE ACCOUNT'),
        ('HPLOAN4 (Restructured NPL)',['SOURCE', 'BRABBR'],                'SOURCE OF BUSINESS', 'RESTRUCTURE NPL ACCOUNT'),
        ('HPLOAN1 (All)',             ['MGINGRP', 'BRABBR'],               'MARGIN OF FINANCE', 'PRODUCT 128,130,380,381,700,705'),
        ('HPLOAN2 (NPL)',             ['MGINGRP', 'BRABBR'],               'MARGIN OF FINANCE', 'NPL ACCOUNT'),
        ('HPLOAN3 (Restructured)',    ['MGINGRP', 'BRABBR'],               'MARGIN OF FINANCE', 'RESTRUCTURE ACCOUNT'),
        ('HPLOAN4 (Restructured NPL)',['MGINGRP', 'BRABBR'],               'MARGIN OF FINANCE', 'RESTRUCTURE NPL ACCOUNT'),
        ('HPLOAN1 (All)',             ['TERMGRP', 'BRABBR'],               'LOAN TERM', 'PRODUCT 128,130,380,381,700,705'),
        ('HPLOAN2 (NPL)',             ['TERMGRP', 'BRABBR'],               'LOAN TERM', 'NPL ACCOUNT'),
        ('HPLOAN3 (Restructured)',    ['TERMGRP', 'BRABBR'],               'LOAN TERM', 'RESTRUCTURE ACCOUNT'),
        ('HPLOAN4 (Restructured NPL)',['TERMGRP', 'BRABBR'],               'LOAN TERM', 'RESTRUCTURE NPL ACCOUNT'),
        ('HPLOAN1 (All)',             ['NEWSEC', 'FINGRP', 'BRABBR'],      'AMT FINANCE', 'PRODUCT 128,130,380,381,700,705'),
        ('HPLOAN2 (NPL)',             ['NEWSEC', 'FINGRP', 'BRABBR'],      'AMT FINANCE', 'NPL ACCOUNT'),
        ('HPLOAN3 (Restructured)',    ['NEWSEC', 'FINGRP', 'BRABBR'],      'AMT FINANCE', 'RESTRUCTURE ACCOUNT'),
        ('HPLOAN4 (Restructured NPL)',['NEWSEC', 'FINGRP', 'BRABBR'],      'AMT FINANCE', 'RESTRUCTURE NPL ACCOUNT'),
        ('HPLOAN1 (All)',             ['NATIONAL', 'STATENM', 'BRABBR'],   'BY STATE', 'PRODUCT 128,130,380,381,700,705'),
        ('HPLOAN2 (NPL)',             ['NATIONAL', 'STATENM', 'BRABBR'],   'BY STATE', 'NPL ACCOUNT'),
        ('HPLOAN3 (Restructured)',    ['NATIONAL', 'STATENM', 'BRABBR'],   'BY STATE', 'RESTRUCTURE ACCOUNT'),
        ('HPLOAN4 (Restructured NPL)',['NATIONAL', 'STATENM', 'BRABBR'],   'BY STATE', 'RESTRUCTURE NPL ACCOUNT'),
        ('HPLOAN1 (All)',             ['NEWSEC', 'CARS', 'MAKE', 'BRABBR'],'BY MAKE OF VEHICLE', 'PRODUCT 128,130,380,381,700,705'),
        ('HPLOAN2 (NPL)',             ['NEWSEC', 'CARS', 'MAKE', 'BRABBR'],'BY MAKE OF VEHICLE', 'NPL ACCOUNT'),
        ('HPLOAN3 (Restructured)',    ['NEWSEC', 'CARS', 'MAKE', 'BRABBR'],'BY MAKE OF VEHICLE', 'RESTRUCTURE ACCOUNT'),
        ('HPLOAN4 (Restructured NPL)',['NEWSEC', 'CARS', 'MAKE', 'BRABBR'],'BY MAKE OF VEHICLE', 'RESTRUCTURE NPL ACCOUNT'),
        ('HPLOAN1 (All)',             ['NEWSEC', 'GOODS', 'BRABBR'],       'BY MAKE OF VEHICLE = OTHERS', 'PRODUCT 128,130,380,381,700,705'),
        ('HPLOAN2 (NPL)',             ['NEWSEC', 'GOODS', 'BRABBR'],       'BY MAKE OF VEHICLE = OTHERS', 'NPL ACCOUNT'),
        ('HPLOAN3 (Restructured)',    ['NEWSEC', 'GOODS', 'BRABBR'],       'BY MAKE OF VEHICLE = OTHERS', 'RESTRUCTURE ACCOUNT'),
        ('HPLOAN4 (Restructured NPL)',['NEWSEC', 'GOODS', 'BRABBR'],       'BY MAKE OF VEHICLE = OTHERS', 'RESTRUCTURE NPL ACCOUNT'),
    ]

    print("\nGenerating summary reports ...")
    summary_count = 0
    for i, (group_key, cols, title, subtitle) in enumerate(report_catalog, start=1):
        frame = groups[group_key]
        if frame.is_empty():
            print(f"  [{i:02d}/{len(report_catalog)}] SKIP  {title} – {subtitle} (no rows)")
            continue
        if 'OTHERS' in title:
            frame = frame.filter(pl.col('MAKE') == 'OTHERS')
            if frame.is_empty():
                print(f"  [{i:02d}/{len(report_catalog)}] SKIP  {title} – {subtitle} (0 OTHERS)")
                continue

        try:
            summary = build_summary(frame, cols)
            filename = (
                f"EIMRESHP_SUMMARY_{i:02d}_"
                f"{title.replace(' ', '_').replace('=', '').replace('__', '_')}.txt"
            )
            write_summary(summary, cols, title, subtitle,
                          os.path.join(OUTPUT_DIR, filename))
            summary_count += 1
            print(f"  [{i:02d}/{len(report_catalog)}] OK    {title} – {subtitle}")
        except Exception as exc:
            print(f"  [{i:02d}/{len(report_catalog)}] FAIL  {title} – {subtitle}: {exc}")
            import traceback; traceback.print_exc()

    print(f"\n  Generated {summary_count} summary reports")

    # -----------------------------------------------------------------------
    # 7. NPL detail report
    # -----------------------------------------------------------------------
    print("\nGenerating NPL detail report ...")
    try:
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
            f.write(f"FILTER     : HP products {HP_PRODUCTS}; "
                    f"LNNOTE ENTITY_CD != '{EXCLUDE_ENTITY}'\n")
            f.write(f"TOTAL NPL ACCOUNTS: {len(detail):,}\n")
            f.write(f"TOTAL NPL BALANCE : RM {detail['BALANCE'].sum():,.2f}\n")
            f.write("=" * 180 + "\n\n")
            f.write(detail_pd.to_string(
                index=False,
                justify='left',
                float_format=lambda x: f'{x:,.2f}',
                na_rep='',
            ))
            f.write("\n\n" + "=" * 180 + "\n")
            f.write("END OF REPORT\n")

        print(f"  {len(detail):,} NPL accounts written "
              f"(balance RM {detail['BALANCE'].sum():,.2f})")

    except Exception as exc:
        print(f"  ERROR generating detail report: {exc}")
        import traceback; traceback.print_exc()

    # -----------------------------------------------------------------------
    # 8. Final summary
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print("EIMRESHP Complete!")
    print(f"{'='*60}")
    print(f"\nOutputs written to: {OUTPUT_DIR}")
    print(f"  - {summary_count} summary reports")
    print(f"  - 1 NPL detail report")
    print(f"\nHP Products: {HP_PRODUCTS}")
    print(f"LNNOTE filter: ENTITY_CD != '{EXCLUDE_ENTITY}'")
    for label, gdf in groups.items():
        print(f"  {label}: {len(gdf):,}")


if __name__ == '__main__':
    main()
