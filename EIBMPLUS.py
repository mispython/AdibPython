#!/usr/bin/env python3
"""
EIBMPLUS - Fixed Deposit Profile Report Generator
Concise Python conversion from SAS using DuckDB and Polars
"""

import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

DEPOSIT_DIR = Path("/data/deposit")
FD_DIR = Path("/data/fd")
OUTPUT_DIR = Path("/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / "EIBMPLUS.txt"

con = duckdb.connect(':memory:')

# ============================================================================
# FDRNGE FORMAT - DEPOSIT RANGE CATEGORIES
# ============================================================================

def get_fdval(curbal):
    """Map balance to deposit range category"""
    if curbal < 10000: return ' 1. BELOW RM 10,000              '
    elif curbal < 20000: return ' 2. RM 10,000  & BELOW RM 20,000 '
    elif curbal < 30000: return ' 3. RM 20,000  & BELOW RM 30,000 '
    elif curbal < 40000: return ' 4. RM 30,000  & BELOW RM 40,000 '
    elif curbal < 50000: return ' 5. RM 40,000  & BELOW RM 50,000 '
    elif curbal < 60000: return ' 6. RM 50,000  & BELOW RM 60,000 '
    elif curbal < 70000: return ' 7. RM 60,000  & BELOW RM 70,000 '
    elif curbal < 80000: return ' 8. RM 70,000  & BELOW RM 80,000 '
    elif curbal < 90000: return ' 9. RM 80,000  & BELOW RM 90,000 '
    elif curbal < 100000: return '10. RM 90,000  & BELOW RM 100,000'
    elif curbal < 500000: return '11. RM 100,000 & BELOW RM 500,000'
    elif curbal < 1000000: return '12. RM 500,000 & BELOW RM 1 MILL '
    elif curbal < 2000000: return '13. RM  1 MILL & BELOW RM 2 MILL '
    elif curbal < 5000000: return '14. RM  2 MILL & BELOW RM 5 MILL '
    else: return '15. RM  5 MILL & ABOVE           '

# ============================================================================
# GET REPORTING DATE
# ============================================================================

reptdate = pl.read_parquet(DEPOSIT_DIR / "reptdate.parquet")["REPTDATE"][0]
REPTYEAR = reptdate.year
REPTMON = f"{reptdate.month:02d}"
REPTDAY = f"{reptdate.day:02d}"
RDATE = reptdate.strftime("%d/%m/%Y")

print(f"Report Date: {RDATE}")

# ============================================================================
# TENURE MAPPING - INTPLAN TO TENURE
# ============================================================================

TENURE_MAP = {
    '01-MONTH   ': [300, 400],
    '02-MONTH   ': [312, 412],
    '03-MONTH   ': [301, 369, 375, 376, 383, 401, 854, 855, 445, 749, 775, 776],
    '04-MONTH   ': [313, 413, 575, 540, 541, 850],
    '05-MONTH   ': [314, 414, 544],
    '06-MONTH   ': [302, 337, 370, 395, 396, 402, 778, 891, 898, 968, 994, 895, 
                    368, 373, 447, 551, 568, 577, 388, 390, 732, 734, 989],
    '07-MONTH   ': [315, 379, 415, 398],
    '08-MONTH   ': [316, 377, 378, 380, 416, 750],
    '09-MONTH   ': [303, 381, 403],
    '10-MONTH   ': [317, 382, 417],
    '11-MONTH   ': [318, 418],
    '12-MONTH   ': [304, 372, 384, 404, 969, 985, 986, 990, 904, 905, 
                    371, 374, 389, 391, 733, 735, 779, 892, 899],
    '13-23-MONTH': [305, 306, 307, 385, 405, 406, 407, 508, 509, 510, 511, 512, 
                    513, 514, 515, 545, 546, 547, 608, 609, 610, 611, 612, 613, 614, 615],
    '24-MONTH   ': [308, 408, 981],
    '25-35-MONTH': [319, 419, 500, 501, 600, 516, 517, 518, 519, 520, 521, 522, 
                    523, 616, 617, 618, 619, 620, 621, 622, 623],
    '36-MONTH   ': [309, 409, 982],
    '37-47-MONTH': [502, 503, 504, 524, 525, 526, 527, 528, 529, 530, 531, 602, 
                    603, 604, 624, 625, 626, 627, 628, 629, 630, 631],
    '48-MONTH   ': [310, 410, 983],
    '49-59-MONTH': [505, 506, 507, 601, 605, 606, 607, 532, 533, 534, 535, 536, 
                    537, 538, 539, 632, 633, 634, 635, 636, 637, 638, 639],
    '60-MONTH   ': [311, 411, 984],
    'ODD TENURE ': [284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 294, 295]
}

def get_tenure(intplan):
    """Map INTPLAN to TENURE category"""
    for tenure, plans in TENURE_MAP.items():
        if intplan in plans:
            return tenure
    return None

# ============================================================================
# PROCESS FD DATA
# ============================================================================

# Load FDA - filtered deposit data for FDVAL classification
fda = pl.read_parquet(DEPOSIT_DIR / "fd.parquet").filter(
    (pl.col("CURBAL") > 0) & (pl.col("PRODUCT").is_in([300, 301, 303, 311, 312]))
).with_columns(
    pl.col("CURBAL").map_elements(get_fdval, return_dtype=pl.Utf8).alias("FDVAL")
).select(["ACCTNO", "FDVAL"])

# Load FD master data
fd = pl.read_parquet(FD_DIR / "fd.parquet").filter(
    ~pl.col("OPENIND").is_in(['B', 'C', 'P'])
).with_columns(
    pl.col("INTPLAN").map_elements(get_tenure, return_dtype=pl.Utf8).alias("TENURE")
).filter(
    pl.col("TENURE").is_not_null()
)

# Merge FDA with FD
fd = fd.join(fda, on="ACCTNO", how="inner").select(
    ["ACCTNO", "TENURE", "FDVAL", "CURBAL"]
).sort(["ACCTNO", "TENURE", "FDVAL"])

# ============================================================================
# CREATE TENURE COLUMNS WITH INDICATORS
# ============================================================================

tenure_list = ['01-MONTH   ', '02-MONTH   ', '03-MONTH   ', '04-MONTH   ', 
               '05-MONTH   ', '06-MONTH   ', '07-MONTH   ', '08-MONTH   ',
               '09-MONTH   ', '10-MONTH   ', '11-MONTH   ', '12-MONTH   ',
               '13-23-MONTH', '24-MONTH   ', '25-35-MONTH', '36-MONTH   ',
               '37-47-MONTH', '48-MONTH   ', '49-59-MONTH', '60-MONTH   ',
               'ODD TENURE ']

# Add account/tenure/value indicators
fd = fd.with_columns([
    pl.lit(1).alias("NOCD"),
    (pl.col("ACCTNO").cum_count().over("ACCTNO") == 1).cast(pl.Int32).alias("NOACCT"),
    (pl.col("ACCTNO").cum_count().over(["ACCTNO", "TENURE", "FDVAL"]) == 1).cast(pl.Int32).alias("INDACCT"),
    (pl.col("ACCTNO").cum_count().over(["ACCTNO", "TENURE"]) == 1).cast(pl.Int32).alias("TENACCT")
])

# Create columns for each tenure
for i, tenure in enumerate(tenure_list, 1):
    fd = fd.with_columns([
        pl.when(pl.col("TENURE") == tenure).then(pl.col("CURBAL")).otherwise(None).alias(f"CUR{i:02d}"),
        pl.when(pl.col("TENURE") == tenure).then(pl.col("NOCD")).otherwise(None).alias(f"CD{i:02d}"),
        pl.when(pl.col("TENURE") == tenure).then(pl.col("TENACCT")).otherwise(None).alias(f"TEN{i:02d}"),
        pl.when(pl.col("TENURE") == tenure).then(pl.col("INDACCT")).otherwise(None).alias(f"IND{i:02d}")
    ])

# ============================================================================
# SECTION 1: OUTSTANDING AMOUNT BY DEPOSIT RANGE AND TENURE
# ============================================================================

cur_cols = [f"CUR{i:02d}" for i in range(1, 22)]
sum_amt = fd.group_by("FDVAL").agg([pl.col(c).sum() for c in cur_cols]).with_columns(
    pl.sum_horizontal(cur_cols).alias("TOTOS")
).sort("FDVAL")

all_sum = sum_amt.select([pl.col(c).sum() for c in ["TOTOS"] + cur_cols])

# ============================================================================
# SECTION 2: NUMBER OF ACCOUNTS AND CDs BY DEPOSIT RANGE AND TENURE
# ============================================================================

cd_cols = [f"CD{i:02d}" for i in range(1, 22)]
ind_cols = [f"IND{i:02d}" for i in range(1, 22)]

sum_fdval = fd.group_by("FDVAL").agg(
    [pl.col("NOACCT").sum()] + [pl.col(c).sum() for c in cd_cols + ind_cols]
).with_columns(
    pl.sum_horizontal(cd_cols).alias("TOTCD")
).sort("FDVAL")

# ============================================================================
# SECTION 3: TOTAL ACCOUNTS AND CDs SUMMARY
# ============================================================================

ten_cols = [f"TEN{i:02d}" for i in range(1, 22)]

sum_tenure = fd.select(
    [pl.col(c).sum() for c in ["NOACCT"] + cd_cols + ten_cols]
).with_columns(
    pl.sum_horizontal(cd_cols).alias("TOTCD")
)

# ============================================================================
# SECTION 4: PLUS FD BY PRODUCT CODE
# ============================================================================

# Zero balance accounts
fd0_prod = pl.read_parquet(DEPOSIT_DIR / "fd.parquet").filter(
    (pl.col("PRODUCT") == 300) & 
    (~pl.col("OPENIND").is_in(['B', 'C', 'P'])) & 
    (pl.col("CURBAL") == 0)
).with_columns([
    pl.lit(1).alias("NOACCT"),
    pl.lit("ZERO BALANCE").alias("CATNAME"),
    pl.lit(1).alias("CATNO")
]).group_by(["CATNAME", "CATNO"]).agg(pl.col("NOACCT").sum())

# Greater than zero balance accounts
fd1_prod = pl.read_parquet(FD_DIR / "fd.parquet").filter(
    (~pl.col("OPENIND").is_in(['B', 'C', 'P'])) & 
    (pl.col("ACCTTYPE") == 300) & 
    (pl.col("CURBAL") > 0)
).with_columns([
    pl.lit("GREATER THAN ZERO BALANCE").alias("CATNAME"),
    pl.lit(2).alias("CATNO")
]).sort("ACCTNO").with_columns([
    pl.lit(1).alias("NOCD"),
    (pl.col("ACCTNO").cum_count().over("ACCTNO") == 1).cast(pl.Int32).alias("NOACCT")
]).group_by(["CATNAME", "CATNO"]).agg([
    pl.col("NOACCT").sum(),
    pl.col("NOCD").sum(),
    pl.col("CURBAL").sum()
])

# Combine and summarize
comb_prod = pl.concat([fd1_prod, fd0_prod], how="diagonal").sort("CATNO").fill_null(0)
sum_plus = comb_prod.select([
    pl.col("NOACCT").sum(),
    pl.col("NOCD").sum(),
    pl.col("CURBAL").sum()
])

# ============================================================================
# WRITE OUTPUT FILE
# ============================================================================

with open(OUTPUT_FILE, 'w') as f:
    # ========================================================================
    # SECTION 1: OUTSTANDING AMOUNT REPORT
    # ========================================================================
    
    f.write("REPORT ID : EIBMPLUS\n")
    f.write("PUBLIC BANK BERHAD\n")
    f.write(f"PROFILE OF PLUS FIXED DEPOSIT BY TENURE AS AT {REPTDAY}/{REPTMON}/{REPTYEAR}\n")
    f.write("OUTSTANDING AMOUNT (RM)\n")
    f.write(";\n")
    f.write("DEPOSIT RANGES;TOTAL AMT O/S RM;;;;;;;;;TENURE\n")
    f.write(";;01-MONTH;02-MONTH;03-MONTH;04-MONTH;05-MONTH;06-MONTH;07-MONTH;08-MONTH;"
            "09-MONTH;10-MONTH;11-MONTH;12-MONTH;13-23-MONTH;24-MONTH;25-35-MONTH;36-MONTH;"
            "37-47-MONTH;48-MONTH;49-59-MONTH;60-MONTH;ODD TENURE;\n")
    f.write(";AMOUNT (RM);" + "AMOUNT (RM);" * 21 + "\n")
    
    for row in sum_amt.iter_rows(named=True):
        values = [row["FDVAL"], f"{row['TOTOS']:,.2f}"] + [f"{row[f'CUR{i:02d}']:,.2f}" for i in range(1, 22)]
        f.write(";".join(values) + ";\n")
    
    # Write totals
    total_row = all_sum.row(0, named=True)
    values = ["TOTAL", f"{total_row['TOTOS']:,.2f}"] + [f"{total_row[f'CUR{i:02d}']:,.2f}" for i in range(1, 22)]
    f.write(";".join(values) + ";\n")
    
    # ========================================================================
    # SECTION 2: NUMBER OF ACCOUNTS AND CDs REPORT
    # ========================================================================
    
    f.write(";\n")
    f.write("REPORT ID : EIBMPLUS\n")
    f.write("PUBLIC BANK BERHAD\n")
    f.write(f"PROFILE OF PLUS FIXED DEPOSIT BY TENURE AS AT {REPTDAY}/{REPTMON}/{REPTYEAR}\n")
    f.write("NO. OF ACCOUNTS AND CDS\n")
    f.write(";\n")
    f.write("DEPOSIT RANGES;;;;;;;;;TENURE\n")
    f.write(";TOTAL;;01-MONTH;;02-MONTH;;03-MONTH;;04-MONTH;;05-MONTH;;06-MONTH;;07-MONTH;;"
            "08-MONTH;;09-MONTH;;10-MONTH;;11-MONTH;;12-MONTH;;13-23-MONTH;;24-MONTH;;"
            "25-35-MONTH;;36-MONTH;;37-47-MONTH;;48-MONTH;;49-59-MONTH;;60-MONTH;;ODD TENURE ;\n")
    f.write(";NO. OF A/C;NO. OF CD;" + "NO. OF A/C;NO. OF CD;" * 21 + "\n")
    
    for row in sum_fdval.iter_rows(named=True):
        values = [row["FDVAL"], f"{row['NOACCT']:,}", f"{row['TOTCD']:,}"]
        for i in range(1, 22):
            values.extend([f"{row[f'IND{i:02d}']:,}", f"{row[f'CD{i:02d}']:,}"])
        f.write(";".join(values) + ";\n")
    
    # Write totals
    total_row = sum_tenure.row(0, named=True)
    values = ["TOTAL", f"{total_row['NOACCT']:,}", f"{total_row['TOTCD']:,}"]
    for i in range(1, 22):
        values.extend([f"{total_row[f'TEN{i:02d}']:,}", f"{total_row[f'CD{i:02d}']:,}"])
    f.write(";".join(values) + ";\n")
    
    # ========================================================================
    # SECTION 3: PLUS FD BY PRODUCT CODE
    # ========================================================================
    
    f.write("REPORT ID : EIBMPLUS\n")
    f.write("PUBLIC BANK BERHAD\n")
    f.write(";\n")
    f.write("PLUS FD(BASED ON PRODUCT CODE); NO. A/C; NO. C/D; AMOUNT\n")
    
    for row in comb_prod.iter_rows(named=True):
        f.write(f"{row['CATNAME']};{row['NOACCT']:,};{row.get('NOCD', 0):,};{row.get('CURBAL', 0):,.2f};\n")
    
    # Write totals
    total_row = sum_plus.row(0, named=True)
    f.write(f"TOTAL PLUS FD;{total_row['NOACCT']:,};{total_row['NOCD']:,};{total_row['CURBAL']:,.2f};\n")

print(f"\nReport generated: {OUTPUT_FILE}")
print("Completed successfully.")

con.close()
