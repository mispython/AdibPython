import polars as pl
import duckdb
from pathlib import Path
import datetime

# Configuration - update these paths as needed
input_base_path = Path("MNILN")
output_path = Path("output")
output_path.mkdir(exist_ok=True)

# Read REPTDATE data (assuming parquet format)
reptdate_df = pl.read_parquet(input_base_path / "REPTDATE.parquet")

# Process REPTDATE similar to SAS DATA step
processed_reptdate = reptdate_df.select([
    pl.col('*'),
    pl.col('REPTDATE').dt.day().alias('day'),
    pl.col('REPTDATE').dt.month().alias('month'),
    pl.col('REPTDATE').dt.year().alias('year')
]).with_columns([
    pl.when(pl.col('day') == 8).then(pl.struct([
        pl.lit(1).alias('sdd'),
        pl.lit('1').alias('wk'),
        pl.lit('4').alias('wk1')
    ]))
    .when(pl.col('day') == 15).then(pl.struct([
        pl.lit(9).alias('sdd'),
        pl.lit('2').alias('wk'),
        pl.lit('1').alias('wk1')
    ]))
    .when(pl.col('day') == 22).then(pl.struct([
        pl.lit(16).alias('sdd'),
        pl.lit('3').alias('wk'),
        pl.lit('2').alias('wk1')
    ]))
    .otherwise(pl.struct([
        pl.lit(23).alias('sdd'),
        pl.lit('4').alias('wk'),
        pl.lit('3').alias('wk1')
    ])).alias('week_info')
]).with_columns([
    pl.col('week_info').struct.field('sdd').alias('SDD'),
    pl.col('week_info').struct.field('wk').alias('WK'),
    pl.col('week_info').struct.field('wk1').alias('WK1'),
    pl.col('month').alias('MM'),
    pl.when(pl.col('WK') == '1').then(
        pl.when(pl.col('month') == 1).then(12).otherwise(pl.col('month') - 1)
    ).otherwise(pl.col('month')).alias('MM1')
]).with_columns([
    pl.datetime(pl.col('year'), pl.col('month'), pl.col('SDD')).alias('SDATE')
]).drop(['day', 'month', 'year', 'week_info'])

# Extract parameters similar to CALL SYMPUT
first_row = processed_reptdate.row(0)
global_vars = {
    'NOWK': first_row['WK'],
    'NOWK1': first_row['WK1'],
    'REPTMON': f"{first_row['MM']:02d}",
    'REPTMON1': f"{first_row['MM1']:02d}",
    'REPTYEAR': str(first_row['REPTDATE'].year),
    'REPTDAY': f"{first_row['REPTDATE'].day:02d}",
    'RDATE': first_row['REPTDATE'].strftime('%d%m%y'),
    'SDATE': first_row['SDATE'].strftime('%d%m%y')
}

print("Global variables:", global_vars)

# Save REPTDATE output
processed_reptdate.write_csv(output_path / "REPTDATE.csv")
processed_reptdate.write_parquet(output_path / "REPTDATE.parquet")

# *** A/C RELEASED FOR THE MTH ***
# Read LNNOTE data from both sources
try:
    lnnote_df1 = pl.read_parquet(input_base_path / "LNNOTE.parquet")
except FileNotFoundError:
    lnnote_df1 = pl.DataFrame()

try:
    lnnote_df2 = pl.read_parquet(Path("MNILNI") / "LNNOTE.parquet")
except FileNotFoundError:
    lnnote_df2 = pl.DataFrame()

# Combine both datasets (similar to SET in SAS)
lnnote_combined = pl.concat([lnnote_df1, lnnote_df2], how="diagonal")

# Process LNNOTE data if not empty
if not lnnote_combined.is_empty():
    # Convert ISSUEDT and apply filters
    # Replace 'HPD' with actual values from your SAS macro
    hpd_values = ['HPD']  # Update with actual HPD values from your SAS macro
    
    lnnote_processed = lnnote_combined.with_columns([
        pl.col('ISSUEDT').cast(pl.Utf8).str.slice(0, 8).str.strptime(pl.Date, '%m%d%Y').alias('ISSDT')
    ]).with_columns([
        pl.col('ISSDT').dt.month().alias('ISSMTH'),
        pl.col('ISSDT').dt.year().alias('ISSYR')
    ]).filter(
        (pl.col('LOANTYPE').is_in(hpd_values)) &
        (pl.col('PAIDIND') != 'P') &
        (pl.col('BALANCE') > 0) &
        (pl.col('ISSMTH') == int(global_vars['REPTMON'])) &
        (pl.col('ISSYR') == int(global_vars['REPTYEAR']))
    )
    
    # Process LOAN data
    loan_df = lnnote_processed.with_columns([
        pl.when(pl.col('COLLDESC').str.slice(37, 1).is_in(['N', 'R']))
        .then(pl.lit('N'))
        .otherwise(pl.lit('S'))
        .alias('NEWSEC'),
        pl.col('NTBRCH').cast(pl.Utf8).alias('BRABBR')
    ])
    
    # Save intermediate outputs
    loan_df.write_csv(output_path / "LOAN.csv")
    loan_df.write_parquet(output_path / "LOAN.parquet")
    
    # *** AVERAGE PER BR BASE ON 30% OF TOTAL ***
    # Create BRHDATA (simulating INFILE) - replace with your actual branch data
    brh_data = [
        {"BRANCH": 2, "BRABBR": "001"},
        {"BRANCH": 3, "BRABBR": "002"},
        # Add more branch data as needed from your actual branch file
    ]
    brh_df = pl.DataFrame(brh_data).filter(
        (pl.col('BRANCH') < 900) & 
        (~pl.col('BRANCH').is_in([1, 99, 100, 218]))
    )
    
    # Calculate number of branches
    nobr = brh_df.height
    print(f"Number of branches: {nobr}")
    
    # Calculate number of accounts
    noacct = loan_df.height
    print(f"Number of accounts: {noacct}")
    
    # Calculate average accounts per branch
    noacrel = round(noacct * 0.3 / nobr) if nobr > 0 else 0
    print(f"Average accounts per branch: {noacrel}")
    
    # *** COMPARE RELEASE PER BR AND AVERAGE PER BR ***
    # Group by BRABBR and count accounts
    loan11_df = loan_df.group_by('BRABBR').agg([
        pl.count().alias('NOACCT')
    ]).with_columns([
        pl.col('NOACCT').mul(0.3).round().cast(pl.Int64).alias('AVGNO_temp')
    ]).with_columns([
        pl.when(pl.col('AVGNO_temp') < noacrel)
        .then(pl.lit(noacrel))
        .otherwise(pl.col('AVGNO_temp'))
        .alias('AVGNO')
    ]).drop('AVGNO_temp')
    
    # Merge back with original loan data
    loan1_df = loan_df.join(loan11_df, on='BRABBR', how='left')
    
    # *** GENERATE TEXT FILE TO LOTUS NOTES SERVER ***
    # Sort and prepare data for output
    loan_sorted = loan1_df.sort('BRABBR')
    
    # Generate text file similar to SAS DATA _NULL_ step
    output_lines = []
    current_brabb = None
    avgacc = 0
    
    for row in loan_sorted.iter_rows(named=True):
        if row['BRABBR'] != current_brabb:
            current_brabb = row['BRABBR']
            avgacc = 0
        
        avgacc += 1
        if avgacc <= row['AVGNO']:
            # Format the line according to SAS PUT specifications
            issmth = f"{row['ISSMTH']:02d}"
            issyr = f"{row['ISSYR']}"
            acctno = str(row.get('ACCTNO', '')).ljust(10)[:10]
            name = str(row.get('NAME', '')).ljust(30)[:30]
            issdt = row['ISSDT'].strftime('%d%m%Y')
            brabb = str(row.get('BRABBR', '')).ljust(3)[:3]
            newsec = str(row.get('NEWSEC', ''))
            
            line = f"{issmth}{issyr}{acctno}{name}{issdt}{brabb}{newsec}"
            output_lines.append(line)
    
    # Write to text file
    with open(output_path / "BRTXT1.txt", "w") as f:
        f.write("\n".join(output_lines))
    
    print(f"Generated text file with {len(output_lines)} records")
    
    # Save final outputs
    loan11_df.write_csv(output_path / "LOAN11.csv")
    loan11_df.write_parquet(output_path / "LOAN11.parquet")
    
    loan1_df.write_csv(output_path / "LOAN1.csv")
    loan1_df.write_parquet(output_path / "LOAN1.parquet")
    
else:
    print("No LNNOTE data found")

print("Processing completed!")
