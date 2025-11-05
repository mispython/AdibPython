import polars as pl
import duckdb
from pathlib import Path
import datetime
import sys
import importlib

# Configuration
fd_path = Path("FD")
output_path = Path("output")
output_path.mkdir(exist_ok=True)

# %INC PGM(PBBELF);
try:
    pbbelf = importlib.import_module('PBBELF')
    pbbelf.process()
except ImportError:
    print("NOTE: PBBELF not available")

# DATA REPTDATE; SET FD.REPTDATE;
reptdate_df = pl.read_parquet(fd_path / "REPTDATE.parquet")

# Process REPTDATE with date calculations
processed_reptdate = reptdate_df.with_columns([
    # T1=REPTDATE + 1; T2=REPTDATE + 2; T3=REPTDATE + 3;
    (pl.col('REPTDATE') + pl.duration(days=1)).alias('T1'),
    (pl.col('REPTDATE') + pl.duration(days=2)).alias('T2'),
    (pl.col('REPTDATE') + pl.duration(days=3)).alias('T3')
])

# Extract parameters - CALL SYMPUT equivalent
first_row = processed_reptdate.row(0)
REPDD = f"{first_row['REPTDATE'].day:02d}"  # Z2.
REPMM = f"{first_row['REPTDATE'].month:02d}"  # Z2.
REPYY = str(first_row['REPTDATE'].year)  # YEAR4.
REPDT = first_row['REPTDATE'].strftime('%d%m%y')  # DDMMYY8.
T = first_row['REPTDATE']
T1 = first_row['T1']
T2 = first_row['T2']
T3 = first_row['T3']

print(f"REPDD: {REPDD}, REPMM: {REPMM}, REPYY: {REPYY}, REPDT: {REPDT}")
print(f"T: {T}, T1: {T1}, T2: {T2}, T3: {T3}")

# DATA FCYT FCYT1 FCYT2 FCYT3 FCYT4; SET FD.FD;
fd_df = pl.read_parquet(fd_path / "FD.parquet").filter(
    (~pl.col('CUSTCD').is_in([77, 78, 95, 96])) &
    (pl.col('CURCODE') != 'MYR') &
    (pl.col('MATDATE').is_not_null())
).with_columns([
    # CURBAL = CURBAL / FORATE;
    (pl.col('CURBAL') / pl.col('FORATE')).alias('CURBAL'),
    
    # MATDAT = INPUT(MATDATE,$20.);
    pl.col('MATDATE').cast(pl.Utf8).str.slice(0, 20).alias('MATDAT')
]).with_columns([
    # YY = SUBSTR(MATDAT,1,4); MM = SUBSTR(MATDAT,5,2); DD = SUBSTR(MATDAT,7,2);
    pl.col('MATDAT').str.slice(0, 4).alias('YY'),
    pl.col('MATDAT').str.slice(4, 2).alias('MM'),
    pl.col('MATDAT').str.slice(6, 2).alias('DD')
]).with_columns([
    # MATDT = MDY(MM,DD,YY);
    pl.when(
        (pl.col('MM').is_not_null()) & 
        (pl.col('DD').is_not_null()) & 
        (pl.col('YY').is_not_null())
    )
    .then(pl.datetime(pl.col('YY').cast(pl.Int64), pl.col('MM').cast(pl.Int64), pl.col('DD').cast(pl.Int64)))
    .otherwise(None)
    .alias('MATDT')
])

# Split data based on maturity dates
fcyt1_df = fd_df.filter(pl.col('MATDT') == T1)
fcyt2_df = fd_df.filter(pl.col('MATDT') == T2)
fcyt3_df = fd_df.filter(pl.col('MATDT') == T3)
fcyt4_df = fd_df.filter(pl.col('MATDT') > T3)

# Save individual datasets
fcyt1_df.write_parquet(output_path / "FCYT1.parquet")
fcyt2_df.write_parquet(output_path / "FCYT2.parquet")
fcyt3_df.write_parquet(output_path / "FCYT3.parquet")
fcyt4_df.write_parquet(output_path / "FCYT4.parquet")

# PROC SUMMARY DATA=FCYT1 NWAY; CLASS CURCODE;
fcyt1_summary = fcyt1_df.group_by('CURCODE').agg([
    pl.col('CURBAL').sum().alias('CURBAL1'),
    pl.col('RATE').sum().alias('RATE1'),
    pl.count().alias('N1')
])

# PROC SUMMARY DATA=FCYT2 NWAY; CLASS CURCODE;
fcyt2_summary = fcyt2_df.group_by('CURCODE').agg([
    pl.col('CURBAL').sum().alias('CURBAL2'),
    pl.col('RATE').sum().alias('RATE2'),
    pl.count().alias('N2')
])

# PROC SUMMARY DATA=FCYT3 NWAY; CLASS CURCODE;
fcyt3_summary = fcyt3_df.group_by('CURCODE').agg([
    pl.col('CURBAL').sum().alias('CURBAL3'),
    pl.col('RATE').sum().alias('RATE3'),
    pl.count().alias('N3')
])

# PROC SUMMARY DATA=FCYT4 NWAY; CLASS CURCODE;
fcyt4_summary = fcyt4_df.group_by('CURCODE').agg([
    pl.col('CURBAL').sum().alias('CURBAL4'),
    pl.col('RATE').sum().alias('RATE4'),
    pl.count().alias('N4')
])

# DATA FCYT; MERGE FCYT1 FCYT2 FCYT3 FCYT4; BY CURCODE;
fcyt_merged = fcyt1_summary.join(fcyt2_summary, on='CURCODE', how='outer')\
                          .join(fcyt3_summary, on='CURCODE', how='outer')\
                          .join(fcyt4_summary, on='CURCODE', how='outer')

# Fill nulls with 0 for calculations
fcyt_processed = fcyt_merged.fill_null(0).with_columns([
    # AVGRATE1 = RATE1/N1; (and similar for others)
    (pl.col('RATE1') / pl.col('N1')).alias('AVGRATE1'),
    (pl.col('RATE2') / pl.col('N2')).alias('AVGRATE2'),
    (pl.col('RATE3') / pl.col('N3')).alias('AVGRATE3'),
    (pl.col('RATE4') / pl.col('N4')).alias('AVGRATE4')
]).with_columns([
    # AVGFIN calculations (running averages divided by 10)
    ((pl.col('AVGRATE1').cum_sum()) / 10).alias('AVGFIN1'),
    ((pl.col('AVGRATE2').cum_sum()) / 10).alias('AVGFIN2'),
    ((pl.col('AVGRATE3').cum_sum()) / 10).alias('AVGFIN3'),
    ((pl.col('AVGRATE4').cum_sum()) / 10).alias('AVGFIN4')
]).with_columns([
    # AVGFIN=SUM(AVGFIN1,AVGFIN2,AVGFIN3,AVGFIN4);
    (pl.col('AVGFIN1') + pl.col('AVGFIN2') + pl.col('AVGFIN3') + pl.col('AVGFIN4')).alias('AVGFIN'),
    
    # TOTFD = SUM(CURBAL1,CURBAL2,CURBAL3,CURBAL4);
    (pl.col('CURBAL1') + pl.col('CURBAL2') + pl.col('CURBAL3') + pl.col('CURBAL4')).alias('TOTFD'),
    
    # TOTAVG = SUM(AVGRATE1,AVGRATE2,AVGRATE3,AVGRATE4); TOTAVG=TOTAVG/4;
    ((pl.col('AVGRATE1') + pl.col('AVGRATE2') + pl.col('AVGRATE3') + pl.col('AVGRATE4')) / 4).alias('TOTAVG')
]).drop(['RATE1', 'RATE2', 'RATE3', 'RATE4', 'N1', 'N2', 'N3', 'N4'])

# Save FCYT dataset
fcyt_processed.write_parquet(output_path / "FCYT.parquet")
fcyt_processed.write_csv(output_path / "FCYT.csv")

# DATA _NULL_; SET FCYT; FILE FDTEXT;
with open(output_path / "FDTEXT.txt", "w") as f:
    f.write(f"FCYFD MATURITY BY CURRENCY AS AT {REPDT}\n")
    f.write(",          ,          ,MATURITY          ,\n")
    f.write(",          T          ,          T1          ,          T2          ,          >T2\n")
    f.write("CURRENCY,          AMOUNT,          AVG RATE,          AMOUNT,          AVG RATE,          " +
            "AMOUNT,          AVG RATE,          AMOUNT,          AVG RATE,          TOTAL FD,          AVG RATE,\n")
    
    for row in fcyt_processed.iter_rows(named=True):
        f.write(f"{row['CURCODE']},          {row['CURBAL1']:.2f},          {row['AVGRATE1']:.2f},          " +
                f"{row['CURBAL2']:.2f},          {row['AVGRATE2']:.2f},          {row['CURBAL3']:.2f},          " +
                f"{row['AVGRATE3']:.2f},          {row['CURBAL4']:.2f},          {row['AVGRATE4']:.2f},          " +
                f"{row['TOTFD']:.2f},          {row['TOTAVG']:.2f},\n")

print("FDTEXT file generated successfully")

# DATA CUST; SET FD.FD;
cust_df = pl.read_parquet(fd_path / "FD.parquet").select([
    'ORIGAMT', 'ORGDATE', 'MATDATE', 'NAME', 'CURCODE', 'RATE', 'CURBAL', 'CUSTCD', 'BRANCH'
]).filter(
    (~pl.col('CUSTCD').is_in([77, 78, 95, 96])) &
    (pl.col('CURCODE') != 'MYR') &
    (pl.col('CURBAL') > 1000000) &
    (pl.col('MATDATE').is_not_null())
).with_columns([
    # BRCHCD = PUT(BRANCH,BRCHCD.);
    pl.col('BRANCH').cast(pl.Utf8).alias('BRCHCD')  # Simplified branch conversion
])

# PROC SORT DATA=CUST; BY DESCENDING CURCODE DESCENDING CURBAL;
cust_sorted = cust_df.sort(['CURCODE', 'CURBAL'], descending=[True, True])

# DATA CUST; SET CUST;
cust_processed = cust_sorted.with_columns([
    # MATDAT = INPUT(MATDATE,$20.);
    pl.col('MATDATE').cast(pl.Utf8).str.slice(0, 20).alias('MATDAT')
]).with_columns([
    # YY = SUBSTR(MATDAT,1,4); MM = SUBSTR(MATDAT,5,2); DD = SUBSTR(MATDAT,7,2);
    pl.col('MATDAT').str.slice(0, 4).alias('YY'),
    pl.col('MATDAT').str.slice(4, 2).alias('MM'),
    pl.col('MATDAT').str.slice(6, 2).alias('DD'),
    
    # ORGDAT=INPUT(SUBSTR(PUT(ORGDATE, Z11.),1,8), MMDDYY10.);
    pl.col('ORGDATE').cast(pl.Utf8).str.zfill(11).str.slice(0, 8)\
        .str.strptime(pl.Date, '%m%d%Y').alias('ORGDAT')
]).with_columns([
    # MATDT = MDY(MM,DD,YY);
    pl.when(
        (pl.col('MM').is_not_null()) & 
        (pl.col('DD').is_not_null()) & 
        (pl.col('YY').is_not_null())
    )
    .then(pl.datetime(pl.col('YY').cast(pl.Int64), pl.col('MM').cast(pl.Int64), pl.col('DD').cast(pl.Int64)))
    .otherwise(None)
    .alias('MATDT')
]).with_columns([
    # MATURE = PUT(MATDT,DDMMYY8.);
    pl.col('MATDT').dt.strftime('%d%m%y').alias('MATURE'),
    
    # ORIGIN = PUT(ORGDAT,DDMMYY8.);
    pl.col('ORGDAT').dt.strftime('%d%m%y').alias('ORIGIN')
]).drop(['ORGDATE', 'MATDATE', 'MATDAT', 'YY', 'MM', 'DD', 'MATDT', 'ORGDAT'])

# Save CUST dataset
cust_processed.write_parquet(output_path / "CUST.parquet")
cust_processed.write_csv(output_path / "CUST.csv")

# DATA _NULL_; SET CUST; FILE FDDETL;
with open(output_path / "FDDETL.txt", "w") as f:
    f.write(f"FCYFD DETAILS BY CUSTOMER(>FCY1MIL) AS AT {REPDT}\n")
    f.write("BRANCH,          CUSTOMER,          SETTLEMENT AMT,          AVG RATE,          " +
            "CURRENCY,          SETTLEMENT DATE,          MATURITY,\n")
    
    for row in cust_processed.iter_rows(named=True):
        f.write(f"{row['BRCHCD']},          {row['NAME']},          {row['ORIGAMT']:.2f},          " +
                f"{row['RATE']:.2f},          {row['CURCODE']},          {row['ORIGIN']},          " +
                f"{row['MATURE']},\n")

print("FDDETL file generated successfully")
print("PROCESSING COMPLETED SUCCESSFULLY")
