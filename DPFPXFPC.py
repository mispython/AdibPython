import polars as pl
import pyreadstat
from datetime import datetime, date, timedelta
from pathlib import Path

# ==================== SETUP ====================
BASE_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/")
FD_PATH = BASE_PATH / "EIBQDISE"
OUTPUT_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIQLIQP")

# Ensure output directory exists
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# ==================== CUSTOM FORMATS ====================
def rem_fmt(value):
    """Format remaining months"""
    if value < 0.255:
        return 'UP TO 1 WK'
    elif 0.255 <= value < 1:
        return '>1 WK - 1 MTH'
    elif 1 <= value < 3:
        return '>1 MTH - 3 MTHS'
    elif 3 <= value < 6:
        return '>3 - 6 MTHS'
    elif 6 <= value < 12:
        return '>6 MTHS - 1 YR'
    else:
        return '> 1 YEAR'

def fcy_fmt(intplan):
    """Format currency type"""
    if 470 <= intplan <= 475:
        return 'USD CURRENCY  '
    return 'OTHER CURRENCY'

# ==================== DATE HELPER FUNCTIONS ====================
def is_leap_year(year):
    """Check if year is leap year"""
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)

def days_in_month(year, month):
    """Get number of days in month"""
    if month == 2:
        return 29 if is_leap_year(year) else 28
    elif month in [4, 6, 9, 11]:
        return 30
    return 31

# ==================== REPTDATE PROCESSING ====================
print("Processing report date...")
# Use yesterday's date
reptdate_val = datetime.now().date() - timedelta(days=1)

day_val = reptdate_val.day
if day_val == 8:
    NOWK = "1"
elif day_val == 15:
    NOWK = "2"
elif day_val == 22:
    NOWK = "3"
else:
    NOWK = "4"

REPTYEAR = str(reptdate_val.year)
REPTMON = f"{reptdate_val.month:02d}"
REPTDAY = f"{day_val:02d}"
RDATE = reptdate_val.strftime("%d%m%Y")

print(f"Report Date: {RDATE}, Week: {NOWK}, Month: {REPTMON}, Year: {REPTYEAR}")

# ==================== FIXED DEPOSITS PROCESSING ====================
print("Processing Fixed Deposits...")

# Read SAS dataset using pyreadstat
sas_file = FD_PATH / "fdmthly.sas7bdat"
if not sas_file.exists():
    print(f"Error: SAS file not found at {sas_file}")
    print("Please ensure the file exists and update BASE_PATH if needed.")
    exit(1)

# Read SAS file
df, meta = pyreadstat.read_sas7bdat(sas_file)
fd_df = pl.from_pandas(df)

print(f"Loaded {len(fd_df)} records from SAS file")

# Filter records
fd_df = fd_df.filter(
    (pl.col("CURBAL") > 0) &
    (~pl.col("OPENIND").is_in(["B", "C", "P"]))
)

print(f"After filtering: {len(fd_df)} records")

# Parse maturity date - handle float values
fd_df = fd_df.with_columns([
    pl.when(pl.col("MATDATE").is_not_null())
    .then(
        pl.col("MATDATE")
        .cast(pl.Utf8)
        .str.replace(r"\.0$", "")
        .str.strptime(pl.Date, "%Y%m%d")
    )
    .otherwise(None)
    .alias("MATDT")
])

# Extract date components for calculations
fd_df = fd_df.with_columns([
    pl.lit(reptdate_val.year).alias("RPYR"),
    pl.lit(reptdate_val.month).alias("RPMTH"),
    pl.lit(reptdate_val.day).alias("RPDAY"),
    pl.col("MATDT").dt.year().alias("MDYR"),
    pl.col("MATDT").dt.month().alias("MDMTH"),
    pl.col("MATDT").dt.day().alias("MDDAY")
])

# Calculate remaining months
def calculate_remmth(row):
    """Calculate remaining months with the same logic as SAS REMMTH macro"""
    if row["OPENIND"] == "D" or (row["MATDT"] - reptdate_val).days < 8:
        return 0.1
    
    rpyr = row["RPYR"]
    rpmth = row["RPMTH"]
    rpday = row["RPDAY"]
    mdyr = row["MDYR"]
    mdmth = row["MDMTH"]
    mdday = row["MDDAY"]
    
    # Adjust MDDAY if > days in report month
    rp_days_in_month = days_in_month(rpyr, rpmth)
    if mdday > rp_days_in_month:
        mdday = rp_days_in_month
    
    # Calculate differences
    remy = mdyr - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday
    
    # Convert to months
    return remy * 12 + remm + remd / rp_days_in_month

# Apply remaining months calculation
fd_df = fd_df.with_columns([
    pl.struct(["OPENIND", "MATDT", "RPYR", "RPMTH", "RPDAY", "MDYR", "MDMTH", "MDDAY"])
    .map_elements(lambda x: calculate_remmth(x), return_dtype=pl.Float64)
    .alias("REMMTH")
])

# Map product code
def map_bic(intplan):
    """Map INTPLAN to BIC - simplified version of FDPROD format"""
    if intplan == 42630:
        return "42630"
    # Add more mappings as needed
    return str(intplan)

fd_df = fd_df.with_columns([
    pl.col("INTPLAN").map_elements(map_bic, return_dtype=pl.Utf8).alias("BIC")
])

# Determine ITEM based on BIC and CUSTCD
fd_df = fd_df.with_columns([
    pl.when(pl.col("BIC") != "42630")
    .then(
        pl.when(pl.col("CUSTCD").is_in([77, 78, 95, 96]))
        .then(pl.lit("A1.15"))
        .otherwise(pl.lit("A1.12"))
    )
    .otherwise(
        pl.when(pl.col("CUSTCD").is_in([77, 78, 95, 96]))
        .then(pl.lit("B1.15"))
        .otherwise(pl.lit("B1.12"))
    )
    .alias("ITEM")
])

# Add constant columns
fd_df = fd_df.with_columns([
    pl.lit("2-RM").alias("PART")
])

# Select and rename columns
fd1 = fd_df.select([
    "BIC", "PART", "ITEM", "REMMTH", pl.col("CURBAL").alias("AMOUNT"), "INTPLAN", "CUSTCD"
])

# ==================== TABULATE REPORT ====================
print("Generating tabulate report...")

# Filter for BIC = 42630
filtered_fd1 = fd1.filter(pl.col("BIC") == "42630")

if len(filtered_fd1) > 0:
    # Apply formatting
    filtered_fd1 = filtered_fd1.with_columns([
        pl.col("REMMTH").map_elements(rem_fmt, return_dtype=pl.Utf8).alias("REMMTH_FMT"),
        pl.col("INTPLAN").map_elements(fcy_fmt, return_dtype=pl.Utf8).alias("INTPLAN_FMT")
    ])

    # Group and aggregate
    grouped_data = filtered_fd1.group_by(["INTPLAN_FMT", "REMMTH_FMT"]).agg([
        pl.col("AMOUNT").sum().alias("AMOUNT_SUM")
    ]).sort(["INTPLAN_FMT", "REMMTH_FMT"])

    # Calculate totals
    total_by_currency = filtered_fd1.group_by("INTPLAN_FMT").agg([
        pl.col("AMOUNT").sum().alias("TOTAL")
    ])

    total_by_maturity = filtered_fd1.group_by("REMMTH_FMT").agg([
        pl.col("AMOUNT").sum().alias("TOTAL")
    ])

    grand_total = filtered_fd1.select(pl.col("AMOUNT").sum().alias("GRAND_TOTAL"))[0, "GRAND_TOTAL"]

    # ==================== GENERATE REPORT ====================
    report_file = OUTPUT_PATH / f"LIQP_REPORT_{RDATE}.txt"

    with open(report_file, 'w') as f:
        f.write(" " * 40 + "PUBLIC BANK BERHAD\n")
        f.write(" " * 40 + "EXPOSURE MARKET RISK AS AT " + 
               f"{RDATE[:2]}/{RDATE[2:4]}/{RDATE[4:]}\n\n")
        
        f.write("FOREIGN FD BY CURRENCY TYPE\n")
        f.write("=" * 80 + "\n")
        
        # Header
        f.write(f"{'INTPLAN':<15}")
        maturity_buckets = ['UP TO 1 WK', '>1 WK - 1 MTH', '>1 MTH - 3 MTHS', 
                           '>3 - 6 MTHS', '>6 MTHS - 1 YR', '> 1 YEAR', 'TOTAL']
        
        for bucket in maturity_buckets:
            f.write(f"{bucket:>15}")
        f.write("\n")
        f.write("-" * 120 + "\n")
        
        # Process each currency type
        currency_types = filtered_fd1["INTPLAN_FMT"].unique().sort()
        
        for currency in currency_types:
            f.write(f"{currency:<15}")
            row_total = 0
            
            for bucket in maturity_buckets[:-1]:  # Exclude 'TOTAL'
                amount = grouped_data.filter(
                    (pl.col("INTPLAN_FMT") == currency) & 
                    (pl.col("REMMTH_FMT") == bucket)
                )["AMOUNT_SUM"].sum()
                
                if amount is None:
                    amount = 0
                
                row_total += amount
                f.write(f"{amount:>15,.2f}")
            
            # Add row total
            f.write(f"{row_total:>15,.2f}\n")
        
        # Add grand total row
        f.write("-" * 120 + "\n")
        f.write(f"{'TOTAL':<15}")
        
        col_totals = []
        for bucket in maturity_buckets[:-1]:
            total = grouped_data.filter(pl.col("REMMTH_FMT") == bucket)["AMOUNT_SUM"].sum()
            if total is None:
                total = 0
            col_totals.append(total)
            f.write(f"{total:>15,.2f}")
        
        f.write(f"{grand_total:>15,.2f}\n")
        f.write("=" * 80 + "\n")
        
        # Summary statistics
        f.write("\nSUMMARY STATISTICS:\n")
        f.write("-" * 40 + "\n")
        f.write(f"Total Records Processed: {len(fd1):,}\n")
        f.write(f"Foreign FD Records (BIC=42630): {len(filtered_fd1):,}\n")
        f.write(f"Total Foreign FD Amount: {grand_total:,.2f}\n")
        
        # Currency distribution
        f.write("\nCURRENCY DISTRIBUTION:\n")
        for row in total_by_currency.iter_rows(named=True):
            percentage = (row["TOTAL"] / grand_total * 100) if grand_total > 0 else 0
            f.write(f"  {row['INTPLAN_FMT']}: {row['TOTAL']:>15,.2f} ({percentage:.1f}%)\n")
        
        # Maturity distribution
        f.write("\nMATURITY DISTRIBUTION:\n")
        for row in total_by_maturity.iter_rows(named=True):
            percentage = (row["TOTAL"] / grand_total * 100) if grand_total > 0 else 0
            f.write(f"  {row['REMMTH_FMT']}: {row['TOTAL']:>15,.2f} ({percentage:.1f}%)\n")

    print(f"Report generated: {report_file}")
else:
    print("No foreign FD data found (BIC=42630)")

# ==================== SUMMARY ====================
print("\n" + "=" * 60)
print("LIQUIDITY PROFILE SUMMARY")
print("=" * 60)
print(f"Report Date: {reptdate_val.strftime('%d/%m/%Y')}")
print(f"Week: {NOWK}, Month: {REPTMON}, Year: {REPTYEAR}")
print(f"Total FD records: {len(fd1):,}")
print(f"Foreign FD records: {len(filtered_fd1):,}")

if len(filtered_fd1) > 0:
    print(f"Total foreign FD amount: {grand_total:,.2f}")
    
    # Breakdown by maturity
    print("\nMaturity Breakdown:")
    for row in total_by_maturity.iter_rows(named=True):
        percentage = (row["TOTAL"] / grand_total * 100) if grand_total > 0 else 0
        print(f"  {row['REMMTH_FMT']}: {row['TOTAL']:>15,.2f} ({percentage:.1f}%)")

print("\nProcessing complete!")
