import polars as pl
import duckdb
from pathlib import Path
import datetime

# Configuration
deposit_path = Path("DEPOSIT")  # This is the SAS library location
output_path = Path("output")
input_file = Path("MAREMUCC5")  # The flat file from RBP2.BKUP.REM.MAREMUC5(0)

output_path.mkdir(exist_ok=True)
deposit_path.mkdir(exist_ok=True)  # Create DEPOSIT directory if it doesn't exist

# DATA REPTDATE (KEEP=REPTDATE);
# REPTDATE=INPUT('01'||PUT(MONTH(TODAY()), Z2.)||PUT(YEAR(TODAY()), 4.), DDMMYY8.)-1;
today = datetime.date.today()
date_string = f"01{today.month:02d}{today.year}"
reptdate = datetime.datetime.strptime(date_string, '%d%m%Y').date() - datetime.timedelta(days=1)

# SELECT(DAY(REPTDATE)); logic
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

# IF WK = '1' THEN DO;
if WK == '1':
    MM1 = MM - 1
    if MM1 == 0:
        MM1 = 12
else:
    MM1 = MM

# MM2 = MM - 1;
MM2 = MM - 1
if MM2 == 0:
    MM2 = 12

SDATE = datetime.date(reptdate.year, MM, SDD)
SDESC = 'PUBLIC BANK BERHAD'

# CALL SYMPUT equivalents
NOWK = WK
REPTMON = f"{MM:02d}"
REPTYEAR = str(reptdate.year)

print(f"NOWK: {NOWK}, REPTMON: {REPTMON}, REPTYEAR: {REPTYEAR}")
print(f"SDESC: {SDESC}")
print(f"REPTDATE: {reptdate}")
print(f"SDATE: {SDATE}")

# Create REPTDATE DataFrame (KEEP=REPTDATE)
reptdate_df = pl.DataFrame({'REPTDATE': [reptdate]})
reptdate_df.write_parquet(output_path / "REPTDATE.parquet")
reptdate_df.write_csv(output_path / "REPTDATE.csv")

# DATA DEPOSIT.UNCLAIM&REPTYEAR DEPOSIT.NOTUNCLAIM&REPTYEAR;
# INFILE UNCLAIM;
filename_unclaim = f"UNCLAIM{REPTYEAR}.parquet"
filename_notunclaim = f"NOTUNCLAIM{REPTYEAR}.parquet"

def read_fixed_width_file(filepath):
    """
    Read the fixed-width flat file MAREMUCC5 with the specified column positions
    """
    try:
        with open(filepath, 'r', encoding='cp1252') as f:
            lines = f.readlines()
        
        # Parse each line according to the SAS INPUT statement
        parsed_data = []
        for line in lines:
            # Ensure line has enough length (at least 115 characters for NAME)
            line = line.rstrip('\n')
            if len(line) < 75:  # Minimum length needed
                continue
                
            try:
                # INPUT @003 ACCTNO PD6. (positions 3-8, 1-indexed)
                # @040 LEDGBAL PD7.2 (positions 40-46, 1-indexed)
                # @047 STATUS $1. (position 47, 1-indexed)
                # @055 PAYMODE $10. (positions 55-64, 1-indexed)
                # @075 NAME $40. (positions 75-114, 1-indexed)
                
                # Extract fields based on positions (converting to 0-indexed for Python)
                acctno_str = line[2:8]  # Positions 3-8 (1-indexed)
                ledgbal_str = line[39:46]  # Positions 40-46 (1-indexed)
                status = line[46:47]  # Position 47 (1-indexed)
                paymode = line[54:64]  # Positions 55-64 (1-indexed)
                name = line[74:114]  # Positions 75-114 (1-indexed)
                
                # Clean and convert fields
                acctno_str = acctno_str.strip()
                ledgbal_str = ledgbal_str.strip()
                status = status.strip()
                paymode = paymode.strip()
                name = name.strip()
                
                # Convert to appropriate types
                acctno = int(acctno_str) if acctno_str else None
                
                # PD7.2 means packed decimal with 2 decimal places
                if ledgbal_str:
                    # Insert decimal point 2 places from the right
                    if len(ledgbal_str) >= 2:
                        ledgbal = float(ledgbal_str[:-2] + '.' + ledgbal_str[-2:])
                    else:
                        ledgbal = float(ledgbal_str) / 100.0
                else:
                    ledgbal = None
                
                parsed_data.append({
                    'ACCTNO': acctno,
                    'LEDGBAL': ledgbal,
                    'STATUS': status,
                    'PAYMODE': paymode,
                    'NAME': name
                })
                
            except (ValueError, IndexError) as e:
                # Skip problematic records
                print(f"Warning: Could not parse line: {line[:50]}... Error: {e}")
                continue
        
        # Create Polars DataFrame
        df = pl.DataFrame(parsed_data)
        
        # Filter out records with missing required fields
        df = df.filter(
            pl.all().is_not_null()
        )
        
        print(f"Successfully read {len(df)} records from {filepath}")
        return df
        
    except FileNotFoundError:
        print(f"File {filepath} not found")
        return None
    except Exception as e:
        print(f"Error reading file: {e}")
        return None

# Read the fixed-width flat file
print(f"\nReading flat file: {input_file}")
unclaim_df = read_fixed_width_file(input_file)

if unclaim_df is not None and not unclaim_df.is_empty():
    print("\nSample of parsed data:")
    print(unclaim_df.head())
    
    # IF STATUS = 'U';
    unclaim_filtered = unclaim_df.filter(pl.col('STATUS') == 'U')
    print(f"\nRecords with STATUS='U': {len(unclaim_filtered)}")
    
    # CATEGORY assignments (matching SAS logic)
    unclaim_with_category = unclaim_filtered.with_columns([
        pl.when(pl.col('PAYMODE').str.slice(0, 1).is_in(['4', '6']))
        .then(pl.lit('SA'))
        .when(pl.col('PAYMODE').str.slice(0, 1).is_in(['3']))
        .then(pl.lit('CA'))
        .when(pl.col('PAYMODE').str.slice(0, 1).is_in(['1', '7']))
        .then(pl.lit('FD'))
        .otherwise(pl.lit('OTHER'))
        .alias('CATEGORY')
    ])
    
    # Split into UNCLAIM and NOTUNCLAIM based on PAYMODE
    valid_paymodes = ['1', '2', '3', '4', '5', '6', '7', '8', '9']
    
    unclaim_valid = unclaim_with_category.filter(
        pl.col('PAYMODE').str.slice(0, 1).is_in(valid_paymodes)
    )
    
    notunclaim_invalid = unclaim_with_category.filter(
        ~pl.col('PAYMODE').str.slice(0, 1).is_in(valid_paymodes)
    )
    
    print(f"\nUNCLAIM valid records (PAYMODE 1-9): {len(unclaim_valid)}")
    print(f"NOTUNCLAIM invalid records: {len(notunclaim_invalid)}")
    
    # PROC SORT DATA=DEPOSIT.UNCLAIM&REPTYEAR; BY PAYMODE;
    if not unclaim_valid.is_empty():
        unclaim_sorted = unclaim_valid.sort('PAYMODE')
        unclaim_sorted.write_parquet(deposit_path / filename_unclaim)
        print(f"\nCreated {filename_unclaim} with {len(unclaim_sorted)} records")
        
        # Save a CSV version for easy viewing
        unclaim_sorted.write_csv(deposit_path / f"UNCLAIM{REPTYEAR}.csv")
    
    # PROC SORT DATA=DEPOSIT.NOTUNCLAIM&REPTYEAR; BY PAYMODE NAME;
    if not notunclaim_invalid.is_empty():
        notunclaim_sorted = notunclaim_invalid.sort(['PAYMODE', 'NAME'])
        notunclaim_sorted.write_parquet(deposit_path / filename_notunclaim)
        print(f"Created {filename_notunclaim} with {len(notunclaim_sorted)} records")
        
        # PROC PRINT; SUM LEDGBAL;
        print("\n" + "="*80)
        print("NOTUNCLAIM DATA SUMMARY:")
        print("="*80)
        print(notunclaim_sorted)
        total_ledgbal = notunclaim_sorted.select(pl.col('LEDGBAL').sum()).row(0)[0]
        print(f"\nTOTAL LEDGBAL: {total_ledgbal:,.2f}")
        print("-"*80)
        
        # Save summary
        notunclaim_sorted.write_csv(deposit_path / f"NOTUNCLAIM{REPTYEAR}.csv")
    
    # PROC SUMMARY DATA=DEPOSIT.UNCLAIM&REPTYEAR; BY PAYMODE; VAR LEDGBAL;
    if not unclaim_valid.is_empty():
        print("\n" + "="*80)
        print("PROCESSING UNCLAIM DATA:")
        print("="*80)
        
        # SUMMARY by PAYMODE
        unclaim_summary = unclaim_sorted.group_by('PAYMODE').agg([
            pl.col('LEDGBAL').sum().alias('LEDGBAL_sum')
        ])
        
        # OUTPUT OUT=UNCLAIM(DROP=_FREQ_ _TYPE_) SUM=;
        unclaim_summary_clean = unclaim_summary.rename({'LEDGBAL_sum': 'LEDGBAL'})
        
        # PROC SORT DATA=DEPOSIT.UNCLAIM&REPTYEAR NODUPKEYS; BY PAYMODE;
        # Get unique PAYMODE records (keeping first occurrence)
        unclaim_deduped = unclaim_sorted.unique(subset=['PAYMODE'], keep='first')
        unclaim_deduped = unclaim_deduped.drop('LEDGBAL')
        
        # DATA DEPOSIT.UNCLAIM&REPTYEAR; MERGE UNCLAIM(IN=A) DEPOSIT.UNCLAIM&REPTYEAR (IN=B DROP=LEDGBAL);
        unclaim_merged = unclaim_deduped.join(
            unclaim_summary_clean, on='PAYMODE', how='inner'
        )
        
        # Save final merged dataset
        unclaim_merged.write_parquet(deposit_path / filename_unclaim)
        unclaim_merged.write_csv(deposit_path / f"UNCLAIM{REPTYEAR}_final.csv")
        
        # PROC PRINT; SUM LEDGBAL;
        print("\nUNCLAIM DATA SUMMARY (AFTER MERGE):")
        print("-"*80)
        print(unclaim_merged)
        total_ledgbal_final = unclaim_merged.select(pl.col('LEDGBAL').sum()).row(0)[0]
        print(f"\nTOTAL LEDGBAL: {total_ledgbal_final:,.2f}")
        print("-"*80)
        
        # Save summary report
        report = unclaim_merged.select([
            'PAYMODE', 'LEDGBAL', 'NAME', 'CATEGORY', 'STATUS'
        ])
        report.write_csv(output_path / f"UNCLAIM_Report_{REPTYEAR}.csv")
        
        # Create a summary statistics file
        summary_stats = unclaim_merged.group_by('CATEGORY').agg([
            pl.count().alias('COUNT'),
            pl.col('LEDGBAL').sum().alias('TOTAL_AMOUNT'),
            pl.col('LEDGBAL').mean().alias('AVERAGE_AMOUNT')
        ])
        summary_stats.write_csv(output_path / f"SUMMARY_BY_CATEGORY_{REPTYEAR}.csv")
        print("\nSummary by CATEGORY:")
        print(summary_stats)
        
else:
    print("\nNo data found or unable to read the input file")
    # Create empty files if no data
    empty_df = pl.DataFrame({
        'ACCTNO': [], 'LEDGBAL': [], 'STATUS': [], 'PAYMODE': [], 
        'NAME': [], 'CATEGORY': []
    })
    empty_df.write_parquet(deposit_path / filename_unclaim)
    empty_df.write_parquet(deposit_path / filename_notunclaim)

print("\n" + "="*80)
print("PROCESSING COMPLETED SUCCESSFULLY")
print("="*80)
print(f"Output files saved in: {deposit_path.absolute()}")
print(f"Report files saved in: {output_path.absolute()}")
