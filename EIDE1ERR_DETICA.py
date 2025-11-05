import polars as pl
import duckdb
from pathlib import Path
import datetime
import sys

# Configuration
output_path = Path("output")
output_path.mkdir(exist_ok=True)

# DATA REPTDATE; INFILE DATEFL LRECL=80 OBS=1;
try:
    # Simulate reading fixed-width file
    # In practice, you would read from actual DATEFL file
    date_data = [
        {"EXTDATE": 10120231124}  # Example date in numeric format
    ]
    date_df = pl.DataFrame(date_data)
    
    # INPUT @01 EXTDATE 11.;
    # REPTDATE = INPUT(SUBSTR(PUT(EXTDATE, Z11.), 1, 8), MMDDYY8.);
    extdate_str = f"{date_df.row(0)['EXTDATE']:011d}"  # Z11. format
    date_str = extdate_str[:8]  # First 8 characters
    reptdate = datetime.datetime.strptime(date_str, '%m%d%Y').date()  # MMDDYY8.
    
except Exception as e:
    print(f"Error reading DATEFL: {e}")
    # Fallback to current date
    reptdate = datetime.date.today()

# CALL SYMPUT equivalent
DAY = f"{reptdate.day:02d}"  # Z2.
MTH = f"{reptdate.month:02d}"  # Z2.
YEAR = str(reptdate.year)  # YEAR4.
TIME = datetime.datetime.now().strftime('%I:%M:%S %p')  # TIMEAMPM11.

print(f"DAY: {DAY}, MTH: {MTH}, YEAR: {YEAR}, TIME: {TIME}")

# DATA DETICA; INFILE DETFL FIRSTOBS=4;
try:
    # Read DETFL file starting from row 4 (skip 3 header rows)
    detica_data = []
    # Simulate reading fixed-width file
    sample_detica = [
        {"DETNAME": "DAILY_TRANSACTION_PROCESS", "DET_CNT": 15000},
        {"DETNAME": "INTEREST_CALCULATION", "DET_CNT": 5000},
        {"DETNAME": "ACCOUNT_RECONCILIATION", "DET_CNT": 7500}
    ]
    detica_df = pl.DataFrame(sample_detica)
    
    # INPUT @01 DETNAME $40. @42 DET_CNT 12.;
    detica_df = detica_df.with_columns([
        pl.col('DETNAME').str.slice(0, 40),  # $40. format
        pl.col('DET_CNT').cast(pl.Int64)     # 12. format
    ])
    
    # IF DET_CNT >= 0;
    detica_df = detica_df.filter(pl.col('DET_CNT') >= 0)
    
    # UID + 1;
    detica_df = detica_df.with_columns([
        (pl.int_range(1, detica_df.height + 1)).alias('UID')
    ])
    
except Exception as e:
    print(f"Error reading DETFL: {e}")
    detica_df = pl.DataFrame({
        'DETNAME': pl.Series([], dtype=pl.Utf8),
        'DET_CNT': pl.Series([], dtype=pl.Int64),
        'UID': pl.Series([], dtype=pl.Int64)
    })

# PROC SORT; BY UID;
detica_sorted = detica_df.sort('UID')

# Save DETICA dataset
detica_sorted.write_parquet(output_path / "DETICA.parquet")
detica_sorted.write_csv(output_path / "DETICA.csv")

# DATA IBM; INFILE IBMFL;
try:
    # Read IBMFL file
    sample_ibm = [
        {"IBMNAME": "DAILY_TRANSACTION_PROCESS", "IBM_CNT": 15000},
        {"IBMNAME": "INTEREST_CALCULATION", "IBM_CNT": 5000},
        {"IBMNAME": "ACCOUNT_RECONCILIATION", "IBM_CNT": 7500}
    ]
    ibm_df = pl.DataFrame(sample_ibm)
    
    # INPUT @01 IBMNAME $25. @29 IBM_CNT 9.;
    ibm_df = ibm_df.with_columns([
        pl.col('IBMNAME').str.slice(0, 25),  # $25. format
        pl.col('IBM_CNT').cast(pl.Int64)     # 9. format
    ])
    
    # IF IBM_CNT >= 0;
    ibm_df = ibm_df.filter(pl.col('IBM_CNT') >= 0)
    
    # UID + 1;
    ibm_df = ibm_df.with_columns([
        (pl.int_range(1, ibm_df.height + 1)).alias('UID')
    ])
    
except Exception as e:
    print(f"Error reading IBMFL: {e}")
    ibm_df = pl.DataFrame({
        'IBMNAME': pl.Series([], dtype=pl.Utf8),
        'IBM_CNT': pl.Series([], dtype=pl.Int64),
        'UID': pl.Series([], dtype=pl.Int64)
    })

# PROC SORT; BY UID;
ibm_sorted = ibm_df.sort('UID')

# Save IBM dataset
ibm_sorted.write_parquet(output_path / "IBM.parquet")
ibm_sorted.write_csv(output_path / "IBM.csv")

# DATA COUNT; MERGE DETICA(IN=A) IBM(IN=B); BY UID;
if not detica_sorted.is_empty() and not ibm_sorted.is_empty():
    count_df = detica_sorted.join(ibm_sorted, on='UID', how='inner')
    
    # DIFF = SUM(DET_CNT,(-1)*IBM_CNT);
    count_df = count_df.with_columns([
        (pl.col('DET_CNT') - pl.col('IBM_CNT')).alias('DIFF')
    ])
    
    # IF DIFF = 0 THEN TALLY = 'Y'; ELSE TALLY = 'N';
    count_df = count_df.with_columns([
        pl.when(pl.col('DIFF') == 0)
        .then(pl.lit('Y'))
        .otherwise(pl.lit('N'))
        .alias('TALLY')
    ])
else:
    count_df = pl.DataFrame({
        'UID': pl.Series([], dtype=pl.Int64),
        'DETNAME': pl.Series([], dtype=pl.Utf8),
        'DET_CNT': pl.Series([], dtype=pl.Int64),
        'IBMNAME': pl.Series([], dtype=pl.Utf8),
        'IBM_CNT': pl.Series([], dtype=pl.Int64),
        'DIFF': pl.Series([], dtype=pl.Int64),
        'TALLY': pl.Series([], dtype=pl.Utf8)
    })

# Save COUNT dataset
count_df.write_parquet(output_path / "COUNT.parquet")
count_df.write_csv(output_path / "COUNT.csv")

# DATA _NULL_; SET COUNT; FILE OUTPUT;
with open(output_path / "OUTPUT.txt", "w") as f:
    if not count_df.is_empty():
        for i, row in enumerate(count_df.iter_rows(named=True), 1):
            if i == 1:
                # Write headers
                f.write(f"REPORT DATE : {DAY}-{MTH}-{YEAR}")
                f.write(f"{'P U B L I C  B A N K  B E R H A D':>65}")
                f.write(f"{'REPORT TIME : ' + TIME:>65}\n")
                f.write(f"{'DETICA DAILY JOBS SCHEDULE':>55}\n")
                f.write("\n")
                f.write("-" * 132 + "\n")
                f.write("Obs   Batch Name (DETICA)                    No. Of Records   No. Of Records   No. Of Records        Tally        Done By\n")
                f.write("                        (DETICA)        (IBM HOST)    (Variance)        (Y/N)    \n")
                f.write("-" * 132 + "\n")
            
            # Write data row
            f.write(f"{row['UID']:3d}   {row['DETNAME']:40}   {row['DET_CNT']:12d}   {row['IBM_CNT']:12d}   {row['DIFF']:12d}   {row['TALLY']:>10}   \n")
            f.write("-" * 132 + "\n")
            f.write("\n")
        
        # Write footer for EOF
        f.write("# Call application team (CIS) if any of the records not TALLY.\n")
    else:
        f.write(f"REPORT DATE : {DAY}-{MTH}-{YEAR}")
        f.write(f"{'P U B L I C  B A N K  B E R H A D':>65}")
        f.write(f"{'REPORT TIME : ' + TIME:>65}\n")
        f.write(f"{'DETICA DAILY JOBS SCHEDULE':>55}\n")
        f.write("\n")
        f.write("NO DATA AVAILABLE\n")
        f.write("# Call application team (CIS) if any of the records not TALLY.\n")

print("OUTPUT file generated successfully")
print("PROCESSING COMPLETED SUCCESSFULLY")
