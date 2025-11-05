import polars as pl
import duckdb
from pathlib import Path
import datetime
import sys

# Configuration
mis_path = Path("MIS")
output_path = Path("output")
output_path.mkdir(exist_ok=True)

# DATA REPTDATE; INFILE DPTRBL LRECL=924 OBS=1;
# Simulating INFILE with fixed-width parsing
try:
    # Read the first record from DPTRBL file (assuming CSV/Parquet for simulation)
    # In real implementation, you'd use proper fixed-width file reading
    dptrbl_data = [
        {"TBDATE": 106}  # Placeholder - actual data would come from fixed-width file
    ]
    dptrbl_df = pl.DataFrame(dptrbl_data)
    
    # INPUT @106 TBDATE PD6.;
    # REPTDATE=INPUT(SUBSTR(PUT(TBDATE, Z11.),1,8),MMDDYY8.);
    # Convert PD6. format to date (simplified)
    tbdate_str = f"{dptrbl_df.row(0)['TBDATE']:011d}"  # Z11.
    date_str = tbdate_str[:8]  # First 8 characters
    reptdate = datetime.datetime.strptime(date_str, '%m%d%Y').date()  # MMDDYY8.
    
    # REPTDATX=REPTDATE-1;
    reptdatx = reptdate - datetime.timedelta(days=1)
    
    # MM=MONTH(REPTDATE);
    mm = reptdate.month
    # MM1=MM-1; IF MM1=0 THEN MM1=12;
    mm1 = mm - 1 if mm > 1 else 12
    
    # CALL SYMPUT equivalent
    RDATE = int(reptdate.strftime('%Y%m%d'))  # PUT(REPTDATE,5.) - assuming YYYYMMDD
    RDATX = int(reptdatx.strftime('%Y%m%d'))   # PUT(REPTDATX,5.)
    XDATE = reptdate.strftime('%d%m%y')        # DDMMYY8.
    REPTDAY = f"{reptdate.day:02d}"            # Z2.
    REPTMON = f"{mm:02d}"                      # Z2.
    REPTMON1 = f"{mm1:02d}"                    # Z2.

except Exception as e:
    print(f"Error reading DPTRBL file: {e}")
    # Fallback to current date logic
    today = datetime.date.today()
    reptdate = today
    reptdatx = today - datetime.timedelta(days=1)
    mm = today.month
    mm1 = mm - 1 if mm > 1 else 12
    
    RDATE = int(reptdate.strftime('%Y%m%d'))
    RDATX = int(reptdatx.strftime('%Y%m%d'))
    XDATE = reptdate.strftime('%d%m%y')
    REPTDAY = f"{reptdate.day:02d}"
    REPTMON = f"{mm:02d}"
    REPTMON1 = f"{mm1:02d}"

print(f"RDATE: {RDATE}, RDATX: {RDATX}, XDATE: {XDATE}")
print(f"REPTDAY: {REPTDAY}, REPTMON: {REPTMON}, REPTMON1: {REPTMON1}")

# MACRO TWORECS equivalent
def tworocs_macro():
    # DATA FCYFDRP; SET MIS.FCYFD&REPTMON MIS.FCYFD&REPTMON1;
    fcyfd_parts = []
    
    try:
        fcyfd1 = pl.read_parquet(mis_path / f"FCYFD{REPTMON}.parquet")
        fcyfd_parts.append(fcyfd1)
    except FileNotFoundError:
        print(f"NOTE: FCYFD{REPTMON}.parquet not found")
    
    try:
        fcyfd2 = pl.read_parquet(mis_path / f"FCYFD{REPTMON1}.parquet")
        fcyfd_parts.append(fcyfd2)
    except FileNotFoundError:
        print(f"NOTE: FCYFD{REPTMON1}.parquet not found")
    
    if fcyfd_parts:
        fcyfdrp_df = pl.concat(fcyfd_parts, how="diagonal")
        # IF (REPTDATE EQ &RDATE) OR (REPTDATE EQ &RDATX);
        fcyfdrp_filtered = fcyfdrp_df.filter(
            (pl.col('REPTDATE') == RDATE) | (pl.col('REPTDATE') == RDATX)
        )
        return fcyfdrp_filtered
    else:
        return pl.DataFrame()

# Execute macro
FCYFDRP = tworocs_macro()

if FCYFDRP.is_empty():
    print("No FCYFDRP data found - exiting")
    sys.exit(1)

# DATA TRFDA;
TRFDA = FCYFDRP.filter(
    (pl.col('REPTDATE') == RDATE) & 
    (pl.col('CURBAL') > 0.00)
).with_columns([
    # IF INTPAY=. THEN INTPAY=0;
    pl.when(pl.col('INTPAY').is_null())
    .then(0.0)
    .otherwise(pl.col('INTPAY'))
    .alias('INTPAY'),
    
    # EXTRTE =ROUND(CURBAL/CURBALF,.0001);
    (pl.col('CURBAL') / pl.col('CURBALF')).round(4).alias('EXTRTE'),
    
    # INTPAYA=INTPAY;
    pl.col('INTPAY').alias('INTPAYA'),
    
    # INTMYRA=ROUND(INTPAY*EXTRTE,.01);
    (pl.col('INTPAY') * pl.col('CURBAL') / pl.col('CURBALF')).round(2).alias('INTMYRA')
]).sort(['ACCTNO', 'CDNO'])

# DATA TRFDB;
TRFDB = FCYFDRP.filter(
    (pl.col('REPTDATE') == (RDATX)) &  # RDATE-1
    (pl.col('CURBAL') > 0.00)
).with_columns([
    # IF INTPAY=. THEN INTPAY=0;
    pl.when(pl.col('INTPAY').is_null())
    .then(0.0)
    .otherwise(pl.col('INTPAY'))
    .alias('INTPAY'),
    
    # EXTRTE =ROUND(CURBAL/CURBALF,.0001);
    (pl.col('CURBAL') / pl.col('CURBALF')).round(4).alias('EXTRTE'),
    
    # INTPAYB=INTPAY;
    pl.col('INTPAY').alias('INTPAYB'),
    
    # INTMYRB=ROUND(INTPAY*EXTRTE,.01);
    (pl.col('INTPAY') * pl.col('CURBAL') / pl.col('CURBALF')).round(2).alias('INTMYRB')
]).select([
    'BRANCH', 'ACCTNO', 'CDNO', 'INTPAYB', 'INTMYRB'
]).sort(['ACCTNO', 'CDNO'])

# DATA TRFD; MERGE TRFDB TRFDA; BY ACCTNO CDNO;
TRFD = TRFDA.join(TRFDB, on=['ACCTNO', 'CDNO'], how='left', suffix='_trfdb').with_columns([
    # TYPE='A';
    pl.lit('A').alias('TYPE'),
    
    # INTVAL =INTPAYA-INTPAYB;
    (pl.col('INTPAYA') - pl.col('INTPAYB')).alias('INTVAL'),
    
    # INTMYR =INTMYRA-INTMYRB;
    (pl.col('INTMYRA') - pl.col('INTMYRB')).alias('INTMYR'),
    
    # TPAMYR =ROUND(CURBAL*INTRTE,.01);
    (pl.col('CURBAL') * pl.col('INTRTE')).round(2).alias('TPAMYR'),
    
    # MDMM =SUBSTR(PUT(MATDATE,Z8.),5,2);
    pl.col('MATDATE').cast(pl.Utf8).str.zfill(8).str.slice(4, 2).alias('MDMM'),
    
    # MDDD =SUBSTR(PUT(MATDATE,Z8.),7,2);
    pl.col('MATDATE').cast(pl.Utf8).str.zfill(8).str.slice(6, 2).alias('MDDD'),
    
    # MDYR =SUBSTR(PUT(MATDATE,Z8.),1,4);
    pl.col('MATDATE').cast(pl.Utf8).str.zfill(8).str.slice(0, 4).alias('MDYR')
]).with_columns([
    # MATDTE =MDY(MDMM,MDDD,MDYR);
    pl.when(
        (pl.col('MDMM').is_not_null()) & 
        (pl.col('MDDD').is_not_null()) & 
        (pl.col('MDYR').is_not_null())
    )
    .then(pl.datetime(pl.col('MDYR').cast(pl.Int64), pl.col('MDMM').cast(pl.Int64), pl.col('MDDD').cast(pl.Int64)))
    .otherwise(None)
    .alias('MATDTE'),
    
    # LMATDATE logic
    pl.when(pl.col('LMATDATE').is_in([None, 0]))
    .then(pl.struct([
        # LMMM =SUBSTR(PUT(ORGDATE,Z9.),1,2);
        pl.col('ORGDATE').cast(pl.Utf8).str.zfill(9).str.slice(0, 2).alias('lmmm'),
        # LMDD =SUBSTR(PUT(ORGDATE,Z9.),3,2);
        pl.col('ORGDATE').cast(pl.Utf8).str.zfill(9).str.slice(2, 2).alias('lmdd'),
        # LMYR =SUBSTR(PUT(ORGDATE,Z9.),5,2);
        pl.col('ORGDATE').cast(pl.Utf8).str.zfill(9).str.slice(4, 2).alias('lmyr')
    ]))
    .otherwise(pl.struct([
        # LMMM =SUBSTR(PUT(LMATDATE,Z11.),1,2);
        pl.col('LMATDATE').cast(pl.Utf8).str.zfill(11).str.slice(0, 2).alias('lmmm'),
        # LMDD =SUBSTR(PUT(LMATDATE,Z11.),3,2);
        pl.col('LMATDATE').cast(pl.Utf8).str.zfill(11).str.slice(2, 2).alias('lmdd'),
        # LMYR =SUBSTR(PUT(LMATDATE,Z11.),5,4);
        pl.col('LMATDATE').cast(pl.Utf8).str.zfill(11).str.slice(4, 4).alias('lmyr')
    ]))
    .alias('date_parts')
]).with_columns([
    pl.col('date_parts').struct.field('lmmm').alias('LMMM'),
    pl.col('date_parts').struct.field('lmdd').alias('LMDD'),
    pl.col('date_parts').struct.field('lmyr').alias('LMYR')
]).with_columns([
    # LMDTE =MDY(LMMM,LMDD,LMYR);
    pl.when(
        (pl.col('LMMM').is_not_null()) & 
        (pl.col('LMDD').is_not_null()) & 
        (pl.col('LMYR').is_not_null())
    )
    .then(pl.datetime(pl.col('LMYR').cast(pl.Int64), pl.col('LMMM').cast(pl.Int64), pl.col('LMDD').cast(pl.Int64)))
    .otherwise(None)
    .alias('LMDTE'),
    
    # TERM calculation (simplified - would need actual term logic)
    pl.lit(0).alias('TERM'),  # Placeholder
    pl.lit(0.0).alias('AVGRTS')  # Placeholder for NOPRINT column
]).drop('date_parts')

# PROC SORT; BY TYPE CURCODE;
TRFD_sorted = TRFD.sort(['TYPE', 'CURCODE'])

# Save outputs
TRFDA.write_parquet(output_path / "TRFDA.parquet")
TRFDB.write_parquet(output_path / "TRFDB.parquet") 
TRFD_sorted.write_parquet(output_path / "TRFD.parquet")
TRFD_sorted.write_csv(output_path / "TRFD.csv")

print(f"PUBLIC BANK BERHAD: REPORT AS AT {XDATE}")
print("OUTSTANDING FOREIGN CURRENCY FD - NONBANK BORROWING")

# PROC REPORT equivalent - generate summary report
report_columns = [
    'TYPE', 'CURCODE', 'ACCTNO', 'NAME', 'CDNO', 'CURBAL', 'CURBALF',
    'INTRTE', 'LMDTE', 'MATDTE', 'INTVAL', 'INTMYR', 'TERM', 'AVGRTS', 'TPAMYR'
]

# Generate summary report similar to PROC REPORT
print("\n" + "="*120)
print("CURR ACCOUNT NO  CUSTOMER   RECP NO  PRINCIPAL/BALANCE MYR EQUIVALENT  INTR/RATE DEPOSIT/ NEXT MA/ DAILY / MYR/EQUIV. TENURE/")
print("                                                                       MATURI    TURITY   INTR.            (DAYS)")
print("                                                                       TY DATE")
print("-"*120)

# Group by CURCODE and calculate subtotals
for curcode in TRFD_sorted['CURCODE'].unique():
    curcode_data = TRFD_sorted.filter(pl.col('CURCODE') == curcode)
    
    for row in curcode_data.select(report_columns).iter_rows(named=True):
        print(f"{row['CURCODE']:4} {row['ACCTNO']:10} {str(row.get('NAME', ''))[:10]:10} {row['CDNO']:6} "
              f"{row['CURBALF']:15.2f} {row['CURBAL']:15.2f} {row['INTRTE']:5.3f} "
              f"{row['LMDTE'].strftime('%d%b%y') if row['LMDTE'] else 'N/A':9} "
              f"{row['MATDTE'].strftime('%d%b%y') if row['MATDTE'] else 'N/A':9} "
              f"{row['INTVAL']:6.2f} {row['INTMYR']:7.2f} {row['TERM']:6}")
    
    # BREAK AFTER CURCODE - subtotal
    curbal_sum = curcode_data['CURBAL'].sum()
    curbalf_sum = curcode_data['CURBALF'].sum()
    intval_sum = curcode_data['INTVAL'].sum()
    intmyr_sum = curcode_data['INTMYR'].sum()
    tpamyr_sum = curcode_data['TPAMYR'].sum()
    avg_rts = (tpamyr_sum / curbal_sum).round(3) if curbal_sum > 0 else 0
    
    print("-"*120)
    print(f"SUB-TOTAL / AVG RATE           {curbal_sum:18.2f} {curbalf_sum:15.2f} {avg_rts:5.3f} "
          f"{intval_sum:7.2f} {intmyr_sum:7.2f}")
    print("-"*120)
    print()

# BREAK AFTER TYPE - grand total
curbal_total = TRFD_sorted['CURBAL'].sum()
curbalf_total = TRFD_sorted['CURBALF'].sum()
intmyr_total = TRFD_sorted['INTMYR'].sum()
tpamyr_total = TRFD_sorted['TPAMYR'].sum()
avg_rts_total = (tpamyr_total / curbal_total).round(3) if curbal_total > 0 else 0

print("-"*120)
print(f"GRAND TOTAL / AVG RATE          {curbal_total:18.2f} {curbalf_total:15.2f} {avg_rts_total:5.3f} "
      f"{intmyr_total:7.2f}")
print("-"*120)

print("PROCESSING COMPLETED SUCCESSFULLY")
