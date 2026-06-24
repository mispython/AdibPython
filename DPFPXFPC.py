import polars as pl
import pyreadstat
from pathlib import Path
import datetime

# Configuration
unclaim_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQUCLM")
mni_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/MNI")
imni_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/IMNI")
output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQUCLM")
output_path.mkdir(exist_ok=True)

# Calculate report date
today = datetime.date.today()
reptdate = datetime.datetime.strptime(f"0101{today.year}", '%d%m%Y').date() - datetime.timedelta(days=1)

# Week calculation
reptday = reptdate.day
if reptday == 8:
    SDD, WK, WK1 = 1, '1', '4'
elif reptday == 15:
    SDD, WK, WK1 = 9, '2', '1'
elif reptday == 22:
    SDD, WK, WK1 = 16, '3', '2'
else:
    SDD, WK, WK1, WK2, WK3 = 23, '4', '3', '2', '1'

MM = reptdate.month
SDATE = datetime.date(reptdate.year, MM, SDD)
REPTMON = f"{MM:02d}"
REPTYEAR = str(reptdate.year)

print(f"REPTDATE: {reptdate}, SDATE: {SDATE}, WEEK: {WK}, MONTH: {REPTMON}")

def read_sas_dataset(filepath, columns=None):
    """Read SAS7BDAT and return Polars DataFrame with lowercase columns"""
    try:
        df, meta = pyreadstat.read_sas7bdat(filepath, usecols=columns)
        pl_df = pl.DataFrame(df).rename({col: col.lower() for col in df.columns})
        # Convert date columns
        for col in pl_df.columns:
            if 'date' in col.lower() or 'dt' in col.lower():
                if pl_df[col].dtype in [pl.Int64, pl.Float64]:
                    pl_df = pl_df.with_columns([
                        pl.when(pl.col(col) > 0)
                        .then(pl.lit(datetime.date(1960, 1, 1)) + pl.duration(days=pl.col(col).cast(pl.Int64)))
                        .otherwise(pl.col(col))
                        .alias(col)
                    ])
        return pl_df, meta
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None, None

# Load UNCLAIM and NOTUNCLAIM
print(f"\nLoading UNCLAIM and NOTUNCLAIM for year {REPTYEAR}")
unclaim_df1, _ = read_sas_dataset(unclaim_path / f"unclaim{REPTYEAR}.sas7bdat")
unclaim_df2, _ = read_sas_dataset(unclaim_path / f"notunclaim{REPTYEAR}.sas7bdat")

# Combine and process UNCLAIM/NONDEBIT
if unclaim_df1 is not None or unclaim_df2 is not None:
    combined = pl.concat([df for df in [unclaim_df1, unclaim_df2] if df is not None and not df.is_empty()], how="diagonal")
    
    if not combined.is_empty():
        # Convert types and add category
        combined = combined.with_columns([
            pl.col('paymode').cast(pl.Utf8),
            pl.col('acctno').cast(pl.Float64),
            pl.when(pl.col('paymode').str.slice(0, 1).is_in(['4', '6'])).then(pl.lit('SA'))
            .when(pl.col('paymode').str.slice(0, 1).is_in(['3'])).then(pl.lit('CA'))
            .when(pl.col('paymode').str.slice(0, 1).is_in(['1', '7'])).then(pl.lit('FD'))
            .otherwise(pl.lit('OTHER')).alias('category')
        ])
        
        # Split into UNCLAIM and NONDEBIT
        valid_paymodes = ['1', '2', '3', '4', '5', '6', '7', '8', '9']
        unclaim = combined.filter(pl.col('paymode').str.slice(0, 1).is_in(valid_paymodes))
        nondebit = combined.filter(~pl.col('paymode').str.slice(0, 1).is_in(valid_paymodes))
        
        print(f"UNCLAIM records: {len(unclaim)}, NONDEBIT records: {len(nondebit)}")
        
        # Process UNCLAIM - summarize by paymode
        if not unclaim.is_empty():
            unclaim_summary = unclaim.group_by('paymode').agg([
                pl.col('ledgbal').sum().alias('ledgbal')
            ])
            unclaim_final = unclaim.drop('ledgbal').unique(subset=['paymode']).join(
                unclaim_summary, on='paymode', how='inner'
            ).unique(subset=['paymode'])
            unclaim_final.write_parquet(output_path / "UNCLAIM_FINAL.parquet")
            print(f"UNCLAIM_FINAL records: {len(unclaim_final)}")
        else:
            unclaim_final = pl.DataFrame()
            
        # Save NONDEBIT
        if not nondebit.is_empty():
            nondebit.sort('paymode').write_parquet(output_path / "NONDEBIT.parquet")
            print(f"NONDEBIT records: {len(nondebit)}")
    else:
        unclaim_final = pl.DataFrame()
        nondebit = pl.DataFrame()
else:
    unclaim_final = pl.DataFrame()
    nondebit = pl.DataFrame()

# Load DEP datasets
print(f"\nLoading DEP datasets")
datasets = []
for path, filename in [(mni_path, f"savg{REPTMON}{WK}.sas7bdat"), 
                       (mni_path, f"curn{REPTMON}{WK}.sas7bdat"),
                       (imni_path, f"savg{REPTMON}{WK}.sas7bdat"),
                       (imni_path, f"curn{REPTMON}{WK}.sas7bdat")]:
    try:
        df, _ = read_sas_dataset(path / filename, columns=['ACCTNO', 'PRODCD', 'COSTCTR'])
        if df is not None:
            df = df.with_columns(pl.col('acctno').cast(pl.Float64))
            datasets.append(df)
            print(f"Loaded {filename} with {len(df)} records")
    except:
        print(f"NOTE: {filename} not found")

# Process DEP
dep_df = pl.concat(datasets, how="diagonal") if datasets else pl.DataFrame()
valid_prodcd = ['42110', '42310', '42120', '42320', '42130', '42132', '42180', '42199', '42699']
dep_filtered = dep_df.filter(pl.col('prodcd').is_in(valid_prodcd)) if not dep_df.is_empty() else pl.DataFrame()
dep_deduped = dep_filtered.unique(subset=['acctno']) if not dep_filtered.is_empty() else pl.DataFrame()
dep_deduped.write_parquet(output_path / "DEP.parquet")
print(f"DEP records: {len(dep_deduped)}")

# Merge UNCLAIM with DEP
if not unclaim_final.is_empty() and not dep_deduped.is_empty():
    unclaim_for_merge = unclaim_final.drop('acctno').with_columns([
        pl.col('paymode').cast(pl.Float64).alias('acctno')
    ])
    dep_merged = dep_deduped.join(unclaim_for_merge, on='acctno', how='right', suffix='_unclaim')
    dep_with_bc = dep_merged.with_columns([
        pl.when(pl.col('prodcd').is_not_null() & pl.col('ledgbal').is_not_null())
        .then(pl.lit('DEBITTED'))
        .otherwise(pl.lit('NOT_FOUND'))
        .alias('bc')
    ]).filter(pl.col('ledgbal').is_not_null())
else:
    dep_with_bc = unclaim_final.with_columns([
        pl.lit('NOT_FOUND').alias('bc'),
        pl.lit(None).cast(pl.Utf8).alias('prodcd'),
        pl.lit(None).cast(pl.Float64).alias('costctr')
    ]) if not unclaim_final.is_empty() else pl.DataFrame()

dep_sorted = dep_with_bc.sort('category') if not dep_with_bc.is_empty() else pl.DataFrame()
dep_sorted.write_parquet(output_path / "DEP_FINAL.parquet")
print(f"DEP_FINAL records: {len(dep_sorted)}")

# Generate combined report with all three sections
def generate_combined_report():
    """Generate combined report with all three sections in production format"""
    lines = []
    timestamp = datetime.datetime.now().strftime("%H:%M %A, %B %d, %Y")
    page_num = 1
    
    # Report 1: DEBITTED
    debitted = dep_sorted.filter(pl.col('bc') == 'DEBITTED')
    if not debitted.is_empty():
        lines.append(f"BANKERS CHEQUE WITH DEBITTED A/C (CONVENTIONAL)                                                    {timestamp}   {page_num}")
        lines.append(" ")
        lines.append(" " * 38 + "BC/DD")
        lines.append(f"{'Obs':<6} {'CATEGORY':<10} {'_TYPE_':<10} {'_FREQ_':<10} {'AMOUNT':>15}")
        lines.append(" ")
        
        summary = debitted.group_by('category').agg([
            pl.count().alias('_FREQ_'),
            pl.col('ledgbal').sum().alias('ledgbal')
        ]).with_columns(pl.lit(1).alias('_TYPE_')).sort('category')
        
        total_amount = 0
        obs = 1
        for row in summary.rows():
            cat, freq, amount, type_val = row
            lines.append(f"{obs:<6} {cat if cat else '':<10} {type_val:<10} {freq:<10} {amount:>15,.2f}")
            total_amount += amount
            obs += 1
        
        lines.append(" " * 37 + "==========")
        lines.append(" " * 37 + f"{total_amount:>15,.2f}")
        lines.append(" ")
        page_num += 1
    
    # Report 2: NOT FOUND
    notfound = dep_sorted.filter(pl.col('bc') == 'NOT_FOUND')
    if not notfound.is_empty():
        lines.append(f"BANKERS CHEQUE WITH DEBITTED A/C NOT FOUND IN FISS (CONV&ISLM)                                     {timestamp}   {page_num}")
        lines.append(" ")
        lines.append(" " * 38 + "BC/DD")
        lines.append(f"{'Obs':<6} {'CATEGORY':<10} {'_TYPE_':<10} {'_FREQ_':<10} {'AMOUNT':>15}")
        lines.append(" ")
        
        summary = notfound.group_by('category').agg([
            pl.count().alias('_FREQ_'),
            pl.col('ledgbal').sum().alias('ledgbal')
        ]).with_columns(pl.lit(1).alias('_TYPE_')).sort('category')
        
        total_amount = 0
        obs = 1
        for row in summary.rows():
            cat, freq, amount, type_val = row
            lines.append(f"{obs:<6} {cat if cat else '':<10} {type_val:<10} {freq:<10} {amount:>15,.2f}")
            total_amount += amount
            obs += 1
        
        lines.append(" " * 37 + "==========")
        lines.append(" " * 37 + f"{total_amount:>15,.2f}")
        lines.append(" ")
        page_num += 1
    
    # Report 3: NON-DEBITTED
    if not nondebit.is_empty():
        nondebit_processed = nondebit.with_columns([
            pl.lit('NON_DEBIT').alias('bc'),
            pl.col('acctno').cast(pl.Float64)
        ])
        
        lines.append(f"BANKERS CHEQUE WITH NON-DEBITTED A/C                                                               {timestamp}   {page_num}")
        lines.append(" ")
        lines.append(" " * 38 + "BC/DD")
        lines.append(f"{'Obs':<6} {'CATEGORY':<10} {'_TYPE_':<10} {'_FREQ_':<10} {'AMOUNT':>15}")
        lines.append(" ")
        
        summary = nondebit_processed.group_by('category').agg([
            pl.count().alias('_FREQ_'),
            pl.col('ledgbal').sum().alias('ledgbal')
        ]).with_columns(pl.lit(1).alias('_TYPE_')).sort('category')
        
        total_amount = 0
        obs = 1
        for row in summary.rows():
            cat, freq, amount, type_val = row
            lines.append(f"{obs:<6} {cat if cat else '':<10} {type_val:<10} {freq:<10} {amount:>15,.2f}")
            total_amount += amount
            obs += 1
        
        lines.append(" " * 37 + "==========")
        lines.append(" " * 37 + f"{total_amount:>15,.2f}")
    
    return "\n".join(lines)

# Generate and save combined report
combined_report = generate_combined_report()

# Print to console
print("\n" + "="*80)
print("COMBINED SUMMARY REPORT")
print("="*80)
print(combined_report)

# Save to single TXT file
report_file = output_path / "BANKERS_CHEQUE_SUMMARY.txt"
with open(report_file, 'w') as f:
    f.write(combined_report)

print(f"\nReport saved to: {report_file}")

# Save processing summary
with open(output_path / "PROCESSING_SUMMARY.txt", 'w') as f:
    f.write("="*80 + "\n")
    f.write("BANKERS CHEQUE PROCESSING SUMMARY\n")
    f.write("="*80 + "\n\n")
    f.write(f"Processing Date: {datetime.datetime.now()}\n")
    f.write(f"Report Date: {reptdate}\n")
    f.write(f"Report Month: {REPTMON}\n")
    f.write(f"Report Year: {REPTYEAR}\n")
    f.write(f"Week: {WK}\n")
    f.write(f"Start Date: {SDATE}\n\n")
    f.write("="*80 + "\n")
    f.write("OUTPUT FILES GENERATED:\n")
    f.write("="*80 + "\n")
    for file in sorted(output_path.glob("*")):
        if file.is_file():
            f.write(f"  {file.name}\n")

print(f"\n{'='*80}")
print(f"All output files saved to: {output_path}")
print("PROCESSING COMPLETED SUCCESSFULLY")
print("="*80)
