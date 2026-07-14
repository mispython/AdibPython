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
    """Format remaining months - Islamic version"""
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
    """Format currency type - Islamic version"""
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
print("Processing Islamic report date...")
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

print(f"Islamic Report Date: {RDATE}, Week: {NOWK}, Month: {REPTMON}, Year: {REPTYEAR}")

# ==================== ISLAMIC FIXED DEPOSITS PROCESSING ====================
print("Processing Islamic Fixed Deposits...")

# Read SAS dataset using pyreadstat
sas_file = FD_PATH / "fd.sas7bdat"
if not sas_file.exists():
    print(f"Error: SAS file not found at {sas_file}")
    print("Please ensure the file exists and update BASE_PATH if needed.")
    exit(1)

# Read SAS file
df, meta = pyreadstat.read_sas7bdat(sas_file)
fd_df = pl.from_pandas(df)

print(f"Loaded {len(fd_df)} records from SAS file")

# ==================== DEBUG: Check INTPLAN values ====================
print("\n" + "=" * 60)
print("DEBUG: Checking INTPLAN values in the data")
print("=" * 60)

# Check unique INTPLAN values
unique_intplan = fd_df.select(pl.col("INTPLAN").unique().sort())
print(f"Unique INTPLAN values found: {len(unique_intplan)}")
print(unique_intplan.head(20))

# Check INTPLAN range for foreign currency (470-475)
foreign_currency_count = fd_df.filter(
    (pl.col("INTPLAN") >= 470) & (pl.col("INTPLAN") <= 475)
).select(pl.len()).item()
print(f"\nRecords with INTPLAN between 470-475 (foreign currency): {foreign_currency_count:,}")

# Check specifically for INTPLAN = 42630
intplan_42630_count = fd_df.filter(pl.col("INTPLAN") == 42630).select(pl.len()).item()
print(f"Records with INTPLAN = 42630: {intplan_42630_count:,}")

# Show some sample foreign currency records
if foreign_currency_count > 0:
    print("\nSample foreign currency records (INTPLAN between 470-475):")
    sample = fd_df.filter(
        (pl.col("INTPLAN") >= 470) & (pl.col("INTPLAN") <= 475)
    ).select(["INTPLAN", "CURBAL", "OPENIND", "CUSTCD"]).head(10)
    print(sample)

print("=" * 60 + "\n")

# Filter records - Islamic specific criteria
fd_df = fd_df.filter(
    (pl.col("CURBAL") > 0) &
    (~pl.col("OPENIND").is_in(["B", "C", "P"]))
)

print(f"After filtering: {len(fd_df)} records")

# Parse maturity date - handle float values
fd_df = fd_df.with_columns([
    pl.when(pl.col("MATDATE").is_not_null())
    .then(
        # Convert float to string, remove decimal, then parse as date
        pl.col("MATDATE")
        .cast(pl.Utf8)
        .str.replace(r"\.0$", "")  # Remove .0 suffix
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

# Calculate remaining months with Islamic considerations
def calculate_remmth_islamic(row):
    """Calculate remaining months with Islamic considerations"""
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
    .map_elements(lambda x: calculate_remmth_islamic(x), return_dtype=pl.Float64)
    .alias("REMMTH")
])

# ==================== IMPROVED BIC MAPPING ====================
# First, let's see what INTPLAN values we have
print("\nAnalyzing INTPLAN values for BIC mapping...")
intplan_stats = fd_df.group_by("INTPLAN").agg([
    pl.len().alias("COUNT"),
    pl.col("CURBAL").sum().alias("TOTAL_BALANCE")
]).sort("INTPLAN")

print(f"Total unique INTPLAN values: {len(intplan_stats)}")
print("\nTop 20 INTPLAN values by count:")
print(intplan_stats.head(20))

# Improved mapping function
def map_bic_islamic(intplan):
    """Map INTPLAN to BIC - Islamic version"""
    # Foreign currency Islamic FDs (based on INTPLAN range 470-475)
    if 470 <= intplan <= 475:
        return "42630"
    # Add other Islamic product mappings as needed
    # Example: if intplan in [42630, 42631, 42632]: return "42630"
    # For all other products, return the INTPLAN as string
    return str(intplan)

fd_df = fd_df.with_columns([
    pl.col("INTPLAN").map_elements(map_bic_islamic, return_dtype=pl.Utf8).alias("BIC")
])

# Check BIC distribution
print("\nBIC distribution after mapping:")
bic_stats = fd_df.group_by("BIC").agg([
    pl.len().alias("COUNT"),
    pl.col("CURBAL").sum().alias("TOTAL_BALANCE")
]).sort("BIC")
print(bic_stats)

# Determine ITEM based on BIC and CUSTCD - Islamic version
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

# Add constant columns - Islamic version
fd_df = fd_df.with_columns([
    pl.lit("2-RM").alias("PART")  # Islamic term for reporting
])

# Select and rename columns
fd1 = fd_df.select([
    "BIC", "PART", "ITEM", "REMMTH", pl.col("CURBAL").alias("AMOUNT"), "INTPLAN", "CUSTCD"
])

# ==================== ISLAMIC TABULATE REPORT ====================
print("\nGenerating Islamic tabulate report...")

# Filter for Islamic BIC = 42630 (foreign currency)
filtered_fd1 = fd1.filter(pl.col("BIC") == "42630")

print(f"Records with BIC='42630' (foreign currency): {len(filtered_fd1)}")

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

    # ==================== GENERATE ISLAMIC REPORT ====================
    report_file = OUTPUT_PATH / f"ISLAMIC_LIQP_REPORT_{RDATE}.txt"

    with open(report_file, 'w') as f:
        f.write(" " * 40 + "PUBLIC ISLAMIC BANK BERHAD\n")
        f.write(" " * 40 + "EXPOSURE MARKET RISK AS AT " + 
               f"{RDATE[:2]}/{RDATE[2:4]}/{RDATE[4:]}\n\n")
        
        f.write("FOREIGN ISLAMIC FD BY CURRENCY TYPE\n")
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
        
        # Islamic summary statistics
        f.write("\nISLAMIC SUMMARY STATISTICS:\n")
        f.write("-" * 40 + "\n")
        f.write(f"Total Islamic FD Records Processed: {len(fd1):,}\n")
        f.write(f"Foreign Islamic FD Records (BIC=42630): {len(filtered_fd1):,}\n")
        f.write(f"Total Foreign Islamic FD Amount: {grand_total:,.2f}\n")
        
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
        
        # Customer type distribution
        f.write("\nCUSTOMER TYPE DISTRIBUTION:\n")
        customer_dist = filtered_fd1.group_by("CUSTCD").agg([
            pl.col("AMOUNT").sum().alias("TOTAL")
        ]).sort("CUSTCD")
        
        for row in customer_dist.iter_rows(named=True):
            cust_type = "INDIVIDUAL" if row["CUSTCD"] in [77, 78, 95, 96] else "NON-INDIVIDUAL"
            percentage = (row["TOTAL"] / grand_total * 100) if grand_total > 0 else 0
            f.write(f"  {cust_type} ({row['CUSTCD']}): {row['TOTAL']:>15,.2f} ({percentage:.1f}%)\n")

    print(f"Islamic report generated: {report_file}")
else:
    print("No Islamic foreign FD data found (BIC=42630)")
    print("\nPossible reasons:")
    print("1. No INTPLAN values between 470-475 (foreign currency)")
    print("2. All foreign currency FDs are filtered out by OPENIND or CURBAL conditions")
    print("3. Different INTPLAN values are used for foreign currency FDs")

# ==================== COMPARISON WITH CONVENTIONAL ====================
print("\n" + "=" * 60)
print("ISLAMIC LIQUIDITY PROFILE SUMMARY")
print("=" * 60)
print(f"Report Date: {reptdate_val.strftime('%d/%m/%Y')}")
print(f"Week: {NOWK}, Month: {REPTMON}, Year: {REPTYEAR}")
print(f"Total Islamic FD records: {len(fd1):,}")
print(f"Foreign Islamic FD records: {len(filtered_fd1):,}")

if len(filtered_fd1) > 0:
    print(f"Total Islamic foreign FD amount: {grand_total:,.2f}")
    
    # Breakdown by maturity
    print("\nIslamic Maturity Breakdown:")
    for row in total_by_maturity.iter_rows(named=True):
        percentage = (row["TOTAL"] / grand_total * 100) if grand_total > 0 else 0
        print(f"  {row['REMMTH_FMT']}: {row['TOTAL']:>15,.2f} ({percentage:.1f}%)")
else:
    print("\nNo foreign currency Islamic FDs found in the data.")

print("\nIslamic processing complete!")
