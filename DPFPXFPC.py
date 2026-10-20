Line 0: len=27  repr=b'300926-REPOSSESSION LISTING\n'
Line 1: len=173  repr=b'H10     NON CAC                    AHQ8598       TOYOTA           HILUX                2KD6835003                               PN131JV2508008599                        2011\n'
Line 2: len=173  repr=b'H10     NON CAC                    AHV1899       P# EIMREPOI_REPO_PROCESSOR.py

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from pathlib import Path
import os
from datetime import datetime, timedelta
import pyreadstat
import pandas as pd

# Import the actual SAS format libraries
import PBBLNFMT
import PBBELF


def normalize_code(series):
    """Normalize numeric/string loan-type-like codes to a clean string form.
    Handles cases where SAS numeric columns come through as e.g. 983.0."""
    return (
        pd.to_numeric(series, errors='coerce')
        .apply(lambda x: str(int(x)) if pd.notna(x) else None)
        .fillna(series.astype(str).str.strip())
    )


def to_int_code(val):
    """Safely convert a value to int for format lookups. Returns None on failure."""
    if pd.isna(val):
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def put_field(line_chars, start_col, value, width):
    """Place `value` at 1-indexed `start_col`, left-justified, padded to `width`.
    Truncates if longer than width. Never raises."""
    s = '' if value is None else str(value)
    s = s[:width].ljust(width)
    idx = start_col - 1
    line_chars[idx:idx + width] = list(s)


def build_report_line(record):
    """Build a single 167-char REPOTXT line matching the SAS PUT statement:
        @001 BRABBR $3.  @009 CAC $20.  @029 REGNO $13.  @043 MAKE $16.
        @060 MODEL $21.  @082 ENGINE $40.  @123 CHASSIS $40.  @164 COLLYEAR $4.
    Total line length = 167 characters (exact SAS parity).
    """
    buf = [' '] * 167
    put_field(buf,   1, record.get('brabbr',   ''),  3)
    put_field(buf,   9, record.get('cac',      ''), 20)
    put_field(buf,  29, record.get('regno',    ''), 13)
    put_field(buf,  43, record.get('make',     ''), 16)
    put_field(buf,  60, record.get('model',    ''), 21)
    put_field(buf,  82, record.get('engine',   ''), 40)
    put_field(buf, 123, record.get('chassis',  ''), 40)
    put_field(buf, 164, record.get('collyear', ''),  4)
    return ''.join(buf)


def main():
    # ------------------------------------------------------------------
    # Step 1: Calculate REPTDATE using current date minus 1 day
    # Must run BEFORE building paths because lnnote_path contains {reptmon}.
    # ------------------------------------------------------------------
    reptdate = datetime.now() - timedelta(days=1)
    day = reptdate.day
    month = reptdate.month
    year = reptdate.year

    if day == 8:
        sdd = 1;  wk = '1';  wk1 = '4'
    elif day == 15:
        sdd = 9;  wk = '2';  wk1 = '1'
    elif day == 22:
        sdd = 16; wk = '3';  wk1 = '2'
    else:
        sdd = 23; wk = '4';  wk1 = '3'

    if wk == '1':
        mm1 = month - 1
        if mm1 == 0:
            mm1 = 12
    else:
        mm1 = month

    sdate = datetime(year, month, sdd)

    nowk = wk
    nowk1 = wk1
    reptmon = f"{month:02d}"
    reptmon1 = f"{mm1:02d}"
    reperyear = str(year)
    reptday = f"{day:02d}"
    # SAS: CALL SYMPUT('RDATE', PUT(REPTDATE, DDMMYY8.));
    # DDMMYY8. produces 'dd/mm/yy' (8 chars with slashes), NOT 'ddmmyy'.
    rdate = reptdate.strftime('%d/%m/%y')
    sdate_str = sdate.strftime('%d/%m/%y')

    # ------------------------------------------------------------------
    # Step 2: Configuration paths
    # ------------------------------------------------------------------
    lnnote_path   = Path(f"/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m{reptmon}.sas7bdat")
    loantemp_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMREPOI/loantemp_iccd2.sas7bdat")
    name8_path    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMREPOI/stg_ln_nam8file.sas7bdat")
    output_path   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMREPOI")

    output_path.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect()

    print(f"Processing date: {reptdate}")
    print(f"Week: {nowk}, Previous Week: {nowk1}")
    print(f"Month: {reptmon}, Previous Month: {reptmon1}")
    print(f"RDate: {rdate}, SDate: {sdate_str}")
    print(f"LNNOTE file: {lnnote_path}")

    # Use the REAL HP macro list from PBBLNFMT, not a hardcoded guess.
    # SAS: WHERE LOANTYPE IN &HP
    hp_values_int = PBBLNFMT.MACROS['HP']             # list of ints
    hp_values_str = [str(x) for x in hp_values_int]   # string form for matching
    print(f"HP loan types from PBBLNFMT ({len(hp_values_int)} codes): {hp_values_int}")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Step 3: Read LNNOTE with usecols (only needed columns)
    # ------------------------------------------------------------------
    print("Reading LNNOTE.sas7bdat (only required columns)...")
    lnnote_df, lnnote_meta = pyreadstat.read_sas7bdat(
        str(lnnote_path),
        usecols=['ACCTNO', 'LOANTYPE', 'NTBRCH', 'COLLDESC', 'COLLYEAR',
                 'BALANCE', 'BORSTAT'],
    )
    print(f"LNNOTE total records: {len(lnnote_df):,}")

    lnnote_df.columns = lnnote_df.columns.str.lower()

    # Filter using the real HP list from PBBLNFMT
    loantype_norm = normalize_code(lnnote_df['loantype'])

    lnnote_filtered = lnnote_df[
        (loantype_norm.isin(hp_values_str)) &
        (lnnote_df['balance'] > 0) &
        (~lnnote_df['borstat'].isin(['F', 'I', 'R']))
    ].copy()

    lnnote_filtered = lnnote_filtered[['acctno', 'loantype', 'ntbrch', 'colldesc', 'collyear']]
    del lnnote_df

    print(f"LNNOTE records after filtering: {len(lnnote_filtered)}")

    # Optional: cap AFTER filtering for a smaller sample test run.
    # lnnote_filtered = lnnote_filtered.head(1000)
    # print(f"LNNOTE limited to 1000 rows for testing: {len(lnnote_filtered)}")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Step 4: Read NAME8
    # ------------------------------------------------------------------
    print("Reading NAME8.sas7bdat (only required columns)...")
    name8_df, _ = pyreadstat.read_sas7bdat(
        str(name8_path),
        usecols=['ACCTNO', 'LINETHRE', 'LINEFOUR'],
    )
    name8_df.columns = name8_df.columns.str.lower()
    name8_df = name8_df.rename(columns={'linethre': 'engine', 'linefour': 'chassis'})
    print(f"NAME8 records: {len(name8_df):,}")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Step 5: Read LOANTEMP
    # ------------------------------------------------------------------
    print("Reading LOANTEMP.sas7bdat (only required columns)...")
    arrear_df, _ = pyreadstat.read_sas7bdat(
        str(loantemp_path),
        usecols=['ACCTNO', 'ARREAR'],
    )
    arrear_df.columns = arrear_df.columns.str.lower()
    print(f"ARREAR records: {len(arrear_df):,}")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Step 6: Merge (SAS: if aa -> LEFT JOIN keeping all LNNOTE rows)
    # ------------------------------------------------------------------
    print("Merging datasets...")
    repo_df = pd.merge(lnnote_filtered, name8_df, on='acctno', how='left')
    repo_df = pd.merge(repo_df, arrear_df, on='acctno', how='left')
    print(f"Merged REPO records: {len(repo_df):,}")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Step 7: Derive fields using the REAL SAS format functions
    #   SAS: BRABBR = PUT(NTBRCH, BRCHCD.);
    #        CAC    = PUT(NTBRCH, CACNAME.);
    #        MAKE   = SUBSTR(COLLDESC, 1, 16);
    #        MODEL  = SUBSTR(COLLDESC, 16, 21);
    #        REGNO  = SUBSTR(COLLDESC, 40, 13);
    # ------------------------------------------------------------------
    print("Processing REPO data (using PBBELF format functions)...")
    processed_records = []

    for _, record in repo_df.iterrows():
        # Convert NTBRCH to int for format lookups
        ntbrch_int = to_int_code(record['ntbrch'])

        # Real SAS format lookups from PBBELF
        if ntbrch_int is not None:
            brabbr = PBBELF.format_brchcd(ntbrch_int) or '   '
            cac    = PBBELF.format_cacname(ntbrch_int) or 'NON CAC'
        else:
            brabbr = '   '
            cac    = 'NON CAC'

        # COLLDESC slices (SAS 1-indexed positions)
        coll_desc = str(record['colldesc']) if pd.notna(record['colldesc']) else ''
        make  = coll_desc[0:16]  if len(coll_desc) >= 16 else coll_desc.ljust(16)
        model = coll_desc[15:36] if len(coll_desc) >= 36 else coll_desc[15:] if len(coll_desc) > 15 else ''
        regno = coll_desc[39:52] if len(coll_desc) >= 52 else coll_desc[39:] if len(coll_desc) > 39 else ''

        engine   = str(record['engine'])   if pd.notna(record['engine'])   else ''
        chassis  = str(record['chassis'])  if pd.notna(record['chassis'])  else ''
        collyear = str(record['collyear'])[:4] if pd.notna(record['collyear']) else ''
        arrear   = float(record['arrear']) if pd.notna(record['arrear'])   else 0

        processed_records.append({
            'acctno':   record['acctno'],
            'loantype': record['loantype'],
            'ntbrch':   ntbrch_int,
            'brabbr':   str(brabbr)[:3],
            'cac':      str(cac)[:20],
            'make':     make[:16],
            'model':    model[:21],
            'regno':    regno[:13],
            'engine':   engine[:40],
            'chassis':  chassis[:40],
            'collyear': collyear[:4],
            'arrear':   arrear,
        })

    REPO_COLUMNS = ['acctno', 'loantype', 'ntbrch', 'brabbr', 'cac', 'make',
                    'model', 'regno', 'engine', 'chassis', 'collyear', 'arrear']

    repo_df_processed = pd.DataFrame(processed_records, columns=REPO_COLUMNS)
    print(f"Processed {len(repo_df_processed)} records")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Step 8: Split into REPO and REPO1
    # ------------------------------------------------------------------
    print("Splitting into REPO and REPO1...")
    repo_filtered = repo_df_processed[repo_df_processed['arrear'] >= 10].copy()

    repo1_loantype_norm = normalize_code(repo_filtered['loantype'])
    repo1_filtered = repo_filtered[repo1_loantype_norm.isin(['983', '993'])].copy()

    print(f"REPO records (ARREAR >= 10): {len(repo_filtered)}")
    print(f"REPO1 records (LOANTYPE 983,993): {len(repo1_filtered)}")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Step 9: Sort by REGNO
    # ------------------------------------------------------------------
    print("Sorting by REGNO...")
    repo_filtered  = repo_filtered.sort_values('regno').reset_index(drop=True)
    repo1_filtered = repo1_filtered.sort_values('regno').reset_index(drop=True)
    print(f"REPO sorted: {len(repo_filtered)} records")
    print(f"REPO1 sorted: {len(repo1_filtered)} records")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Step 10: Write REPOTXT.txt — exact 167-char SAS layout
    # ------------------------------------------------------------------
    print("Creating REPOTXT.txt...")
    repotxt_file = output_path / "REPOTXT.txt"
    with open(repotxt_file, 'w') as f:
        if len(repo_filtered) > 0:
            f.write(f"{rdate}-REPOSSESSION LISTING\n")

        for _, record in repo_filtered.iterrows():
            f.write(build_report_line(record) + '\n')

    print(f"Created REPOTXT file: {repotxt_file}")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Step 11: Write REPOTXT1.txt — same 167-char layout, different header
    # ------------------------------------------------------------------
    print("Creating REPOTXT1.txt...")
    repotxt1_file = output_path / "REPOTXT1.txt"
    with open(repotxt1_file, 'w') as f:
        if len(repo1_filtered) > 0:
            f.write(f"{rdate}-REPOSSESSION LISTING (983,993)\n")

        for _, record in repo1_filtered.iterrows():
            f.write(build_report_line(record) + '\n')

    print(f"Created REPOTXT1 file: {repotxt1_file}")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print(f"\n{'=' * 60}")
    print("PROCESSING COMPLETED SUCCESSFULLY!")
    print(f"{'=' * 60}")
    print("Summary:")
    print(f"  Processing Date: {reptdate.strftime('%Y-%m-%d')}")
    print(f"  RDate: {rdate}")
    print(f"  SDate: {sdate_str}")
    print(f"  Week: {nowk}, Previous Week: {nowk1}")
    print(f"  Month: {reptmon}, Previous Month: {reptmon1}")
    print(f"  HP loan types used: {hp_values_int}")
    print(f"{'=' * 60}")
    print(f"  Total LNNOTE records after filtering: {len(lnnote_filtered)}")
    print(f"  REPO records (ARREAR >= 10): {len(repo_filtered)}")
    print(f"  REPO1 records (983,993): {len(repo1_filtered)}")
    print(f"{'=' * 60}")

    if len(repo_filtered) > 0:
        loantype_summary = repo_filtered['loantype'].value_counts()
        print("\nLoan type distribution in REPO:")
        for lt, count in loantype_summary.items():
            print(f"  {lt}: {count} records")
        print(f"{'=' * 60}")

    if len(repo_filtered) > 0:
        print("\nSample REPO output (first 3 records):")
        for i in range(min(3, len(repo_filtered))):
            r = repo_filtered.iloc[i]
            print(f"  {i+1}. BRABBR={r['brabbr']}  CAC={r['cac']!r}  "
                  f"REGNO={r['regno']}  LOANTYPE={r['loantype']}  ARREAR={r['arrear']}")

    conn.close()
    print(f"\nOutput files saved to: {output_path}")
    print(f"  - REPOTXT.txt")
    print(f"  - REPOTXT1.txt")


if __name__ == "__main__":
    main()ROTON           INSPIRA              4B10HD0260                               PL1CY3STRCF012574                        2012\n'

NAME8 columns: ['acctno', 'linethre', 'linefour']
NAME8 sample rows:
         acctno      linethre           linefour
0  2.000001e+09  K24W62971081  PMHCR2640ED601117
1  2.000002e+09    S6D-059918  MNAFB2473CN009980
2  2.000003e+09       G59B34N  PM2M502G002314151
3  2.000003e+09    S4PEUX2762  PL1BH3LTRGG033894
4  2.000003e+09  G399E-514905  PMYUG0420G0015805
