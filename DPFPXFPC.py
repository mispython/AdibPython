# EIMREPOI_REPO_PROCESSOR.py

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from pathlib import Path
import os
from datetime import datetime, timedelta
import pyreadstat
import pandas as pd


def normalize_code(series):
    """Normalize numeric/string loan-type-like codes to a clean string form.
    Handles cases where SAS numeric columns come through as e.g. 983.0."""
    return (
        pd.to_numeric(series, errors='coerce')
        .apply(lambda x: str(int(x)) if pd.notna(x) else None)
        .fillna(series.astype(str).str.strip())
    )


def main():
    # ------------------------------------------------------------------
    # Step 1 (moved up): Calculate REPTDATE using current date minus 1 day
    # This must run BEFORE building paths, because loan_path contains
    # {reptmon} in its filename.
    # ------------------------------------------------------------------
    reptdate = datetime.now() - timedelta(days=1)
    day = reptdate.day
    month = reptdate.month
    year = reptdate.year

    # Implement SELECT(DAY(REPTDATE)) logic from SAS
    if day == 8:
        sdd = 1
        wk = '1'
        wk1 = '4'
    elif day == 15:
        sdd = 9
        wk = '2'
        wk1 = '1'
    elif day == 22:
        sdd = 16
        wk = '3'
        wk1 = '2'
    else:
        sdd = 23
        wk = '4'
        wk1 = '3'

    # Calculate MM1 (SAS logic)
    if wk == '1':
        mm1 = month - 1
        if mm1 == 0:
            mm1 = 12
    else:
        mm1 = month

    # Calculate SDATE
    sdate = datetime(year, month, sdd)

    # Set macro variables equivalent (SAS SYMPUT)
    nowk = wk
    nowk1 = wk1
    reptmon = f"{month:02d}"
    reptmon1 = f"{mm1:02d}"
    reperyear = str(year)
    reptday = f"{day:02d}"
    rdate = reptdate.strftime('%d%m%y')  # DDMMYY format
    sdate_str = sdate.strftime('%d%m%y')

    # ------------------------------------------------------------------
    # Step 2: Configuration using pathlib
    # Paths point directly at the .sas7bdat FILES (not directories).
    # loan_path uses the {reptmon} macro now that reptmon is known.
    # ------------------------------------------------------------------
    lnnote_path   = Path(f"/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS//enrh_ln_note_m{reptmon}.sas7bdat") #/sas/ln/dwh/enrichment/enrh_ln_note_m{reptmon}.sas7bdat
    loantemp_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMREPOI/loantemp_iccd2.sas7bdat") #/dwh/dwh_m/tempsource/loantemp_iccd2.sas7bdat
    name8_path    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMREPOI/stg_ln_nam8file.sas7bdat") #/sas/ln/dwh/staging/stg_ln_nam8file.sas7bdat
    output_path   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMREPOI") #/host_pq/mis/output/report/

    # Create output directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)

    # Connect to DuckDB
    conn = duckdb.connect()

    print(f"Processing date: {reptdate}")
    print(f"Week: {nowk}, Previous Week: {nowk1}")
    print(f"Month: {reptmon}, Previous Month: {reptmon1}")
    print(f"RDate: {rdate}, SDate: {sdate_str}")
    print(f"LNNOTE file: {lnnote_path}")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Step 3: Read and process LNNOTE with filters (SAS PROC SORT + WHERE)
    # ------------------------------------------------------------------
    print("Reading LNNOTE.sas7bdat...")
    lnnote_df, lnnote_meta = pyreadstat.read_sas7bdat(str(lnnote_path))
    print(f"LNNOTE total records: {len(lnnote_df):,}")

    # Convert column names to lowercase
    lnnote_df.columns = lnnote_df.columns.str.lower()

    # HP loan types - these would come from &HP macro variable in SAS
    hp_values = ['983', '993', '984', '994']

    # Filter on the FULL dataset first, and normalize loantype so
    # numeric-vs-string mismatches (e.g. 983.0) don't silently drop everything.
    loantype_norm = normalize_code(lnnote_df['loantype'])

    lnnote_filtered = lnnote_df[
        (loantype_norm.isin(hp_values)) &
        (lnnote_df['balance'] > 0) &
        (~lnnote_df['borstat'].isin(['F', 'I', 'R']))
    ].copy()

    # Keep only needed columns (KEEP=ACCTNO LOANTYPE NTBRCH COLLDESC COLLYEAR)
    lnnote_filtered = lnnote_filtered[['acctno', 'loantype', 'ntbrch', 'colldesc', 'collyear']]

    print(f"LNNOTE records after filtering: {len(lnnote_filtered)}")

    # Optional: cap AFTER filtering for a smaller sample test run.
    # lnnote_filtered = lnnote_filtered.head(1000)
    # print(f"LNNOTE limited to 1000 rows for testing: {len(lnnote_filtered)}")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Step 4: Read NAME8 (KEEP=ACCTNO LINETHRE LINEFOUR)
    # ------------------------------------------------------------------
    print("Reading NAME8.sas7bdat...")
    name8_df, name8_meta = pyreadstat.read_sas7bdat(str(name8_path))
    name8_df.columns = name8_df.columns.str.lower()
    print(f"NAME8 total records: {len(name8_df):,}")

    # Keep only needed columns
    name8_df = name8_df[['acctno', 'linethre', 'linefour']]
    # Rename columns (SAS RENAME=(LINETHRE=ENGINE LINEFOUR=CHASSIS))
    name8_df = name8_df.rename(columns={'linethre': 'engine', 'linefour': 'chassis'})

    print(f"NAME8 records kept: {len(name8_df)}")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Step 5: Read ARREAR (KEEP=ACCTNO ARREAR)
    # ------------------------------------------------------------------
    print("Reading LOANTEMP.sas7bdat...")
    arrear_df, arrear_meta = pyreadstat.read_sas7bdat(str(loantemp_path))
    arrear_df.columns = arrear_df.columns.str.lower()
    print(f"ARREAR total records: {len(arrear_df):,}")

    # Keep only needed columns
    arrear_df = arrear_df[['acctno', 'arrear']]

    print(f"ARREAR records kept: {len(arrear_df)}")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Step 6: Merge datasets (SAS DATA REPO with MERGE and BY ACCTNO)
    # SAS: merge lnnote(in=aa) name8 arrear; by acctno; if aa;
    # "if aa" = keep EVERY LNNOTE row, matched or not -> plain LEFT JOIN.
    # ------------------------------------------------------------------
    print("Merging datasets...")

    repo_df = pd.merge(
        lnnote_filtered,
        name8_df,
        on='acctno',
        how='left'
    )

    repo_df = pd.merge(
        repo_df,
        arrear_df,
        on='acctno',
        how='left'
    )

    print(f"Merged REPO records: {len(repo_df)}")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Step 7: Process REPO data - add derived fields (SAS DATA REPO step)
    # ------------------------------------------------------------------
    print("Processing REPO data...")

    processed_records = []

    for idx, record in repo_df.iterrows():
        # BRABBR - would need format lookup (BRCHCD.), using NTBRCH as placeholder
        ntbrch_val = record['ntbrch'] if pd.notna(record['ntbrch']) else None
        brabbr = f"BR{str(ntbrch_val)[:3]}" if ntbrch_val else "000"

        # CAC - would need format lookup (CACNAME.)
        cac = f"CAC{str(ntbrch_val)[:5]}" if ntbrch_val else "UNKNOWN"

        # Extract vehicle details from COLLDESC (SAS 1-indexed positions)
        coll_desc = str(record['colldesc']) if pd.notna(record['colldesc']) else ''

        # SAS SUBSTR(COLLDESC,1,16)
        make = coll_desc[0:16] if len(coll_desc) >= 16 else coll_desc.ljust(16)

        # SAS SUBSTR(COLLDESC,16,21) -> Python 0-indexed: start 15, take 21
        model = coll_desc[15:36] if len(coll_desc) >= 36 else coll_desc[15:] if len(coll_desc) > 15 else ''

        # SAS SUBSTR(COLLDESC,40,13) -> Python 0-indexed: start 39, take 13
        regno = coll_desc[39:52] if len(coll_desc) >= 52 else coll_desc[39:] if len(coll_desc) > 39 else ''

        # Handle None values
        engine = str(record['engine']) if pd.notna(record['engine']) else ''
        chassis = str(record['chassis']) if pd.notna(record['chassis']) else ''
        collyear = str(record['collyear'])[:4] if pd.notna(record['collyear']) else ''
        arrear = float(record['arrear']) if pd.notna(record['arrear']) else 0

        processed_record = {
            'acctno': record['acctno'],
            'loantype': record['loantype'],
            'ntbrch': ntbrch_val,
            'brabbr': brabbr[:3],
            'cac': cac[:20],
            'make': make[:16],
            'model': model[:21],
            'regno': regno[:13],
            'engine': engine[:40],
            'chassis': chassis[:40],
            'collyear': collyear[:4],
            'arrear': arrear
        }
        processed_records.append(processed_record)

    # Explicit column list so an empty list still yields a valid DataFrame
    REPO_COLUMNS = ['acctno', 'loantype', 'ntbrch', 'brabbr', 'cac', 'make',
                    'model', 'regno', 'engine', 'chassis', 'collyear', 'arrear']

    repo_df_processed = pd.DataFrame(processed_records, columns=REPO_COLUMNS)
    print(f"Processed {len(repo_df_processed)} records")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Step 8: Split into REPO and REPO1 (SAS DATA REPO REPO1)
    # ------------------------------------------------------------------
    print("Splitting into REPO and REPO1...")
    # First filter: IF ARREAR GE 10
    repo_filtered = repo_df_processed[repo_df_processed['arrear'] >= 10].copy()

    # REPO1: IF LOANTYPE IN (983,993) -- normalized, numeric-safe comparison
    repo1_loantype_norm = normalize_code(repo_filtered['loantype'])
    repo1_filtered = repo_filtered[repo1_loantype_norm.isin(['983', '993'])].copy()

    print(f"REPO records (ARREAR >= 10): {len(repo_filtered)}")
    print(f"REPO1 records (LOANTYPE 983,993): {len(repo1_filtered)}")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Step 9: Sort by REGNO (PROC SORT BY REGNO)
    # ------------------------------------------------------------------
    print("Sorting by REGNO...")
    repo_filtered = repo_filtered.sort_values('regno').reset_index(drop=True)
    repo1_filtered = repo1_filtered.sort_values('regno').reset_index(drop=True)
    print(f"REPO sorted: {len(repo_filtered)} records")
    print(f"REPO1 sorted: {len(repo1_filtered)} records")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Step 10: Create fixed-width text output for REPO
    # ------------------------------------------------------------------
    print("Creating REPOTXT.txt...")
    repotxt_file = output_path / "REPOTXT.txt"
    with open(repotxt_file, 'w') as f:
        if len(repo_filtered) > 0:
            f.write(f"{rdate}-REPOSSESSION LISTING\n")

        for _, record in repo_filtered.iterrows():
            line = ''
            line += f"{record['brabbr']:<3}"     # @001 (0-index:0)
            line += ' ' * 5                      # pad to @009
            line += f"{record['cac']:<20}"       # @009 (0-index:8)
            line += ' ' * 7                      # pad to @029
            line += f"{record['regno']:<13}"     # @029 (0-index:28)
            line += ' ' * 1                      # pad to @043
            line += f"{record['make']:<16}"      # @043 (0-index:42)
            line += f"{record['model']:<21}"     # @060 (0-index:59)
            line += ' ' * 1                      # pad to @082
            line += f"{record['engine']:<40}"    # @082 (0-index:81)
            line += ' ' * 1                      # pad to @123
            line += f"{record['chassis']:<40}"   # @123 (0-index:122)
            line += ' ' * 1                      # pad to @164
            line += f"{record['collyear']:<4}"   # @164 (0-index:163)
            f.write(line + '\n')

    print(f"Created REPOTXT file: {repotxt_file}")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Step 11: Create fixed-width text output for REPO1
    # ------------------------------------------------------------------
    print("Creating REPOTXT1.txt...")
    repotxt1_file = output_path / "REPOTXT1.txt"
    with open(repotxt1_file, 'w') as f:
        if len(repo1_filtered) > 0:
            f.write(f"{rdate}-REPOSSESSION LISTING (983,993)\n")

        for _, record in repo1_filtered.iterrows():
            line = ''
            line += f"{record['brabbr']:<3}"     # @001 (0-index:0)
            line += ' ' * 5                      # pad to @009
            line += f"{record['cac']:<20}"       # @009 (0-index:8)
            line += ' ' * 7                      # pad to @029
            line += f"{record['regno']:<13}"     # @029 (0-index:28)
            line += ' ' * 1                      # pad to @043
            line += f"{record['make']:<16}"      # @043 (0-index:42)
            line += f"{record['model']:<21}"     # @060 (0-index:59)
            line += ' ' * 1                      # pad to @082
            line += f"{record['engine']:<40}"    # @082 (0-index:81)
            line += ' ' * 1                      # pad to @123
            line += f"{record['chassis']:<40}"   # @123 (0-index:122)
            line += ' ' * 1                      # pad to @164
            line += f"{record['collyear']:<4}"   # @164 (0-index:163)
            f.write(line + '\n')

    print(f"Created REPOTXT1 file: {repotxt1_file}")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Step 12: Also save as Parquet and CSV for reference
    # ------------------------------------------------------------------
    print("Saving Parquet and CSV files...")
    if len(repo_filtered) > 0:
        repo_arrow = pa.Table.from_pandas(repo_filtered)
        pq.write_table(repo_arrow, output_path / "REPO.parquet")
        csv.write_csv(repo_arrow, output_path / "REPO.csv")
        print("Saved REPO.parquet and REPO.csv")

    if len(repo1_filtered) > 0:
        repo1_arrow = pa.Table.from_pandas(repo1_filtered)
        pq.write_table(repo1_arrow, output_path / "REPO1.parquet")
        csv.write_csv(repo1_arrow, output_path / "REPO1.csv")
        print("Saved REPO1.parquet and REPO1.csv")
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
            record = repo_filtered.iloc[i]
            print(f"  {i+1}. REGNO: {record['regno']}, "
                  f"LOANTYPE: {record['loantype']}, ARREAR: {record['arrear']}")

    conn.close()
    print(f"\nAll output files saved to: {output_path}")


if __name__ == "__main__":
    main()
