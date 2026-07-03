import polars as pl
from pathlib import Path
import datetime
import pyreadstat
import re
import os

# Configuration
deposit_path = Path("/host_pq/mis/input")
mni_sa_path = Path("/dwh/dp_sa")      # SAVG files
mni_ca_path = Path("/dwh/dp_ca")      # CURN files
imni_sa_path = Path("/dwh/idp_sa")    # ISAVG files
imni_ca_path = Path("/dwh/idp_ca")    # ICURN files
output_path = Path("/host/mis/output")
output_path.mkdir(exist_ok=True)
deposit_path.mkdir(exist_ok=True)

# Production date - Calculate based on today's date
today = datetime.date.today()
date_string = f"01{today.month:02d}{today.year}"
reptdate = datetime.datetime.strptime(date_string, '%d%m%Y').date() - datetime.timedelta(days=1)
print(f"*** PRODUCTION MODE - Date: {reptdate} ***")

# Date logic
reptday = reptdate.day
if reptday == 8: 
    SDD, WK = 1, '1'
elif reptday == 15: 
    SDD, WK = 9, '2'
elif reptday == 22: 
    SDD, WK = 16, '3'
else: 
    SDD, WK = 23, '4'

MM = reptdate.month
NOWK, REPTMON, REPTYEAR = WK, f"{MM:02d}", str(reptdate.year)
print(f"NOWK: {NOWK}, REPTMON: {REPTMON}, REPTYEAR: {REPTYEAR}")

# Create REPTDATE
pl.DataFrame({'REPTDATE': [reptdate]}).write_parquet(output_path / "REPTDATE.parquet")
pl.DataFrame({'REPTDATE': [reptdate]}).write_csv(output_path / "REPTDATE.csv")

def clean_text(text):
    return ''.join(ch for ch in text if 31 < ord(ch) < 127).strip() if text else ""

def parse_remit_file(filepath):
    """Parse fixed-width REMIT file (MAREMORE)"""
    records = []
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                raw = line.rstrip('\n\r')
                if not raw.strip(): continue
                
                # Extract fields based on SAS positions (0-indexed)
                acctno = clean_text(raw[2:8]) if len(raw) > 8 else ""
                cheqno = clean_text(raw[8:14]) if len(raw) > 14 else ""
                issyy = clean_text(raw[26:30]) if len(raw) > 30 else ""
                issmm = clean_text(raw[31:33]) if len(raw) > 33 else ""
                issdd = clean_text(raw[34:36]) if len(raw) > 36 else ""
                issbranch = clean_text(raw[36:39]) if len(raw) > 39 else ""
                ledgbal_str = clean_text(raw[39:48]) if len(raw) > 48 else ""
                status = clean_text(raw[46:48]) if len(raw) > 48 else ""
                paymode = clean_text(raw[54:64]) if len(raw) > 64 else ""
                name = clean_text(raw[74:114]) if len(raw) > 114 else ""
                
                # Try regex for ACCTNO if parsing failed
                if not acctno or len(acctno) < 6:
                    match = re.search(r'([0-9]{6,7})', raw)
                    if match: acctno = match.group(1)
                
                # Parse LEDGBAL (PD7.2 format)
                try:
                    ledgbal = float(ledgbal_str) if ledgbal_str else 0.0
                except:
                    ledgbal = 0.0
                
                if acctno or name:
                    records.append({'ACCTNO': acctno, 'CHEQNO': cheqno, 'ISSYY': issyy, 
                                   'ISSMM': issmm, 'ISSDD': issdd, 'ISSBRANCH': issbranch,
                                   'LEDGBAL': ledgbal, 'STATUS': status, 'PAYMODE': paymode, 'NAME': name})
        
        if not records: return pl.DataFrame()
        
        df = pl.DataFrame(records).with_columns([
            pl.col('ACCTNO').cast(pl.Int64, strict=False),
            pl.col('CHEQNO').cast(pl.Int64, strict=False),
            pl.col('ISSBRANCH').cast(pl.Int64, strict=False)
        ]).filter(pl.col('ACCTNO').is_not_null())
        
        print(f"Parsed {df.height} records from MAREMORE")
        return df
    except Exception as e:
        print(f"Error parsing REMIT: {e}")
        return pl.DataFrame()

# Process REMIT file
remit_file = deposit_path / "MAREMORE"
remit_df = parse_remit_file(remit_file) if remit_file.exists() else pl.DataFrame()

if not remit_df.is_empty():
    # Create ISSDTE and CATEGORY
    remit_df = remit_df.with_columns([
        pl.when(pl.all_horizontal([pl.col('ISSMM').is_not_null(), pl.col('ISSDD').is_not_null(), 
                                   pl.col('ISSYY').is_not_null()]))
        .then(pl.concat_str(['ISSMM', 'ISSDD', 'ISSYY']).str.strptime(pl.Date, '%m%d%Y', strict=False))
        .alias('ISSDTE'),
        pl.when(pl.col('PAYMODE').str.slice(0, 1).is_in(['4','5','6'])).then(pl.lit('SA'))
        .when(pl.col('PAYMODE').str.slice(0, 1).is_in(['3'])).then(pl.lit('CA'))
        .when(pl.col('PAYMODE').str.slice(0, 1).is_in(['1','7'])).then(pl.lit('FD'))
        .otherwise(pl.lit('OTHER')).alias('CATEGORY')
    ])
    
    # Split into REMIT and NONDEBIT
    valid_paymodes = ['1','2','3','4','5','6','7','8','9']
    remit_valid = remit_df.filter(pl.col('PAYMODE').str.slice(0, 1).is_in(valid_paymodes)).drop(['ISSMM','ISSDD','ISSYY'])
    nondebit_invalid = remit_df.filter(~pl.col('PAYMODE').str.slice(0, 1).is_in(valid_paymodes))
    
    # Save
    remit_valid.write_parquet(deposit_path / "REMIT.parquet")
    nondebit_invalid.write_parquet(output_path / "NONDEBIT.parquet")
    print(f"REMIT: {remit_valid.height}, NONDEBIT: {nondebit_invalid.height}")
    
    # Create REMIT_FINAL (summary by PAYMODE)
    remit_final = (remit_valid.group_by('PAYMODE').agg(pl.col('LEDGBAL').sum().alias('LEDGBAL'))
                   .join(remit_valid.unique(subset=['PAYMODE']).drop('LEDGBAL'), on='PAYMODE', how='inner')
                   .unique(subset=['PAYMODE']))
    remit_final.write_parquet(output_path / "REMIT_FINAL.parquet")
else:
    remit_valid = nondebit_invalid = remit_final = pl.DataFrame()

# Read SAS files
def read_sas(filepath):
    try:
        if not filepath.exists(): 
            print(f"File not found: {filepath}")
            return None
        df, _ = pyreadstat.read_sas7bdat(filepath)
        return pl.DataFrame(df).select(['ACCTNO', 'PRODCD', 'COSTCTR'])
    except Exception as e:
        print(f"Error reading {filepath.name}: {e}")
        return None

# Load all SAS files with new naming convention: sa{REPTMON}{NOWK}{REPTYEAR} and ca{REPTMON}{NOWK}{REPTYEAR}
# Example: sa1242026.sas7bdat, ca1242026.sas7bdat
savg_filename = f"sa{REPTMON}{NOWK}{REPTYEAR}.sas7bdat"
curn_filename = f"ca{REPTMON}{NOWK}{REPTYEAR}.sas7bdat"
isavg_filename = f"sa{REPTMON}{NOWK}{REPTYEAR}.sas7bdat"
icurn_filename = f"ca{REPTMON}{NOWK}{REPTYEAR}.sas7bdat"

print(f"\nLooking for SAS files:")
print(f"SAVG: {savg_filename} in {mni_sa_path}")
print(f"CURN: {curn_filename} in {mni_ca_path}")
print(f"ISAVG: {isavg_filename} in {imni_sa_path}")
print(f"ICURN: {icurn_filename} in {imni_ca_path}")

savg = read_sas(mni_sa_path / savg_filename)
curn = read_sas(mni_ca_path / curn_filename)
isavg = read_sas(imni_sa_path / isavg_filename)
icurn = read_sas(imni_ca_path / icurn_filename)

# Combine and filter DEP - Check if any DataFrames were loaded
loaded_dfs = [d for d in [savg, curn, isavg, icurn] if d is not None and not d.is_empty()]
dep_deduped = pl.DataFrame()

if loaded_dfs:
    print(f"\nCombining {len(loaded_dfs)} datasets...")
    dep_df = pl.concat(loaded_dfs, how="diagonal")
    print(f"Combined DEP dataset with {dep_df.height} records")
    
    valid_prodcd = ['42110','42310','42120','42320','42130','42132','42180','42199','42699']
    dep_filtered = dep_df.filter(pl.col('PRODCD').is_in(valid_prodcd))
    print(f"Filtered DEP with valid PRODCD: {dep_filtered.height} records")
    
    dep_deduped = dep_filtered.unique(subset=['ACCTNO'])
    if not dep_deduped.is_empty():
        dep_deduped = dep_deduped.with_columns(pl.col('ACCTNO').cast(pl.Int64))
        dep_deduped.write_parquet(output_path / "DEP_deduped.parquet")
        dep_deduped.write_csv(output_path / "DEP_deduped.csv")
        print(f"DEP deduped: {dep_deduped.height} records")
else:
    print("No DEP datasets loaded")

# Merge and generate reports
if not remit_final.is_empty() and not dep_deduped.is_empty():
    # Merge DEP and REMIT
    merged = (dep_deduped.join(remit_final.with_columns(pl.col('PAYMODE').cast(pl.Int64).alias('ACCTNO')), 
                              on='ACCTNO', how='right')
              .with_columns([
                  pl.when(pl.col('PRODCD').is_not_null() & pl.col('LEDGBAL').is_not_null())
                  .then(pl.lit('DEBITTED')).otherwise(pl.lit('NOT_FOUND')).alias('BC')
              ]).with_columns([
                  pl.col('PRODCD').fill_null('UNKNOWN'),
                  pl.col('COSTCTR').fill_null(0)
              ]).sort('CATEGORY'))
    
    merged.write_parquet(output_path / "DEP_SORTED.parquet")
    merged.write_csv(output_path / "DEP_SORTED.csv")
    
    # Report 1: Debitted
    debitted = merged.filter((pl.col('BC') == 'DEBITTED') & ((pl.col('COSTCTR') < 3000) | (pl.col('COSTCTR') > 3999)))
    if not debitted.is_empty():
        summary = debitted.group_by('CATEGORY').agg(pl.col('LEDGBAL').sum()).sort('CATEGORY')
        print("\n=== REPORT 1: DEBITTED A/C (CONVENTIONAL) ===")
        print(summary)
        print(f"TOTAL: {summary.select(pl.col('LEDGBAL').sum()).row(0)[0]:,.2f}\n")
        summary.write_parquet(output_path / "DEBITTED_SUMMARY.parquet")
        summary.write_csv(output_path / "DEBITTED_SUMMARY.csv")
        debitted.write_parquet(output_path / "DEBITTED_FILTERED.parquet")
        debitted.write_csv(output_path / "DEBITTED_FILTERED.csv")
    
    # Report 2: Not Found
    notfound = merged.filter(
        (pl.col('BC') == 'NOT_FOUND') & 
        ~((pl.col('ACCTNO') > 3700000000) & (pl.col('ACCTNO') < 3999999999) |
          (pl.col('ACCTNO') > 4700000000) & (pl.col('ACCTNO') < 4999999999) |
          (pl.col('ACCTNO') > 6700000000) & (pl.col('ACCTNO') < 6999999999) |
          (pl.col('ACCTNO') > 1700000000) & (pl.col('ACCTNO') < 1999999999) |
          (pl.col('ACCTNO') > 7700000000) & (pl.col('ACCTNO') < 7999999999))
    )
    if not notfound.is_empty():
        summary = notfound.group_by('CATEGORY').agg(pl.col('LEDGBAL').sum()).sort('CATEGORY')
        print("=== REPORT 2: NOT FOUND IN FISS ===")
        print(summary)
        print(f"TOTAL: {summary.select(pl.col('LEDGBAL').sum()).row(0)[0]:,.2f}\n")
        summary.write_parquet(output_path / "NOTFOUND_SUMMARY.parquet")
        summary.write_csv(output_path / "NOTFOUND_SUMMARY.csv")
        notfound.write_parquet(output_path / "NOTFOUND_FILTERED.parquet")
        notfound.write_csv(output_path / "NOTFOUND_FILTERED.csv")

# Report 3: Non-Debit
if not nondebit_invalid.is_empty():
    nondebit = nondebit_invalid.with_columns([
        pl.lit('NON_DEBIT').alias('BC'),
        pl.col('PAYMODE').alias('ACCTNO')
    ])
    summary = nondebit.group_by('CATEGORY').agg(pl.col('LEDGBAL').sum()).sort('CATEGORY')
    print("=== REPORT 3: NON-DEBITTED A/C ===")
    print(summary)
    print(f"TOTAL: {summary.select(pl.col('LEDGBAL').sum()).row(0)[0]:,.2f}\n")
    summary.write_parquet(output_path / "NONDEBIT_SUMMARY.parquet")
    summary.write_csv(output_path / "NONDEBIT_SUMMARY.csv")
    nondebit.write_parquet(output_path / "NONDEBIT_PROCESSED.parquet")
    nondebit.write_csv(output_path / "NONDEBIT_PROCESSED.csv")

# Create final consolidated summary
all_summaries = []
for name, df in [('DEBITTED A/C (CONVENTIONAL)', 'debitted'), 
                  ('NOT FOUND IN FISS', 'notfound'), 
                  ('NON-DEBITTED A/C', 'nondebit')]:
    if name == 'DEBITTED A/C (CONVENTIONAL)' and 'debitted' in locals() and not debitted.is_empty():
        all_summaries.append(debitted.group_by('CATEGORY').agg(pl.col('LEDGBAL').sum()).with_columns(pl.lit(name).alias('REPORT')))
    elif name == 'NOT FOUND IN FISS' and 'notfound' in locals() and not notfound.is_empty():
        all_summaries.append(notfound.group_by('CATEGORY').agg(pl.col('LEDGBAL').sum()).with_columns(pl.lit(name).alias('REPORT')))
    elif name == 'NON-DEBITTED A/C' and 'nondebit' in locals() and not nondebit.is_empty():
        all_summaries.append(nondebit.group_by('CATEGORY').agg(pl.col('LEDGBAL').sum()).with_columns(pl.lit(name).alias('REPORT')))

if all_summaries:
    final_summary = pl.concat(all_summaries)
    print("\n=== FINAL CONSOLIDATED SUMMARY ===")
    print(final_summary)
    total = final_summary.select(pl.col('LEDGBAL').sum()).row(0)[0]
    print(f"\nGRAND TOTAL: {total:,.2f}")
    final_summary.write_parquet(output_path / "FINAL_SUMMARY.parquet")
    final_summary.write_csv(output_path / "FINAL_SUMMARY.csv")

print("\n" + "="*80)
print("PROCESSING COMPLETED SUCCESSFULLY")
print("="*80)

# List all output files
print("\nOUTPUT FILES CREATED:")
for path in [deposit_path, output_path]:
    print(f"\nIn {path}:")
    for file in sorted(path.glob("*")):
        if file.suffix in ['.parquet', '.csv']:
            print(f"  - {file.name}")
