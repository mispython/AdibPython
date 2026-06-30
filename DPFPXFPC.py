# eibdcitx.py
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from datetime import date, datetime
import pyreadstat
import os

# -------------------------------------------------------------------
# Step 1: Reporting Date Setup
# -------------------------------------------------------------------
today = date.today()
REPTDAY = f"{today.day:02d}"
REPTMON = f"{today.month:02d}"
REPTYEAR = f"{today.year % 100:02d}"
RDATE = today.strftime("%d/%m/%Y")
INDATES = today

day = today.day
if 1 <= day <= 8:
    WK = "1"
elif 9 <= day <= 15:
    WK = "2"
elif 16 <= day <= 22:
    WK = "3"
else:
    WK = "4"

print(f"Running EIBDCITX for {RDATE} (WK={WK})")

# -------------------------------------------------------------------
# Step 2: Load raw datasets
# -------------------------------------------------------------------
# Load DPFL (fixed width format)
dpfl = pl.read_csv("DPFL.txt", 
                   has_header=False,
                   schema={
                       'TICKETNO': pl.Utf8,
                       'CUSTNAME': pl.Utf8,
                       'NEWIC': pl.Utf8,
                       'CUSTCODE': pl.Int64,
                       'INVCURAC': pl.Int64,
                       'ALTCURAC': pl.Int64,
                       'ACCINT': pl.Float64
                   },
                   fixed_width=True,
                   widths=[7, 26, 20, 5, 11, 11, 15],
                   newline_character='\n')

# Load EQFL (pipe delimited)
eqfl = pl.read_csv("EQFL.txt", separator="|", 
                   schema_overrides={
                       'STARTDT': pl.Utf8,
                       'MATDT': pl.Utf8,
                       'ACCINTRM': pl.Float64,
                       'ACCINTAMT': pl.Float64,
                       'TOTINTAMT': pl.Float64,
                       'PREMPAID': pl.Float64,
                       'PREMREC': pl.Float64
                   })

# Load CRA (fixed width format)
cra = pl.read_csv("CRA.txt",
                  has_header=False,
                  fixed_width=True,
                  widths=[3, 60, 6, 140, 7, 10, 10, 7, 2, 3, 8, 2],
                  newline_character='\n',
                  schema={
                      'BRANCH': pl.Utf8,
                      'CUSTICKETNO': pl.Utf8,
                      'INVCURAC': pl.Int64,
                      'CUSTNAME': pl.Utf8,
                      'INVAMT': pl.Float64,
                      'STARTDT': pl.Utf8,
                      'MATDT': pl.Utf8,
                      'DCIRT': pl.Float64,
                      'TENOR': pl.Int64,
                      'INV_STATUS': pl.Utf8,
                      'ACCINT': pl.Float64,
                      'CUSTCODE_DB2': pl.Int64
                  })

# Load EQRATE (SAS dataset)
eqrate, meta = pyreadstat.read_sas7bdat(f"EQRATE{REPTYEAR}{REPTMON}{REPTDAY}.sas7bdat")
eqrt = pl.DataFrame(eqrate)

# Load MNITB datasets
mnitb_saving = pl.read_csv("MNITB_SAVING.txt", schema={'ACCTNO': pl.Int64, 'CUSTCODE': pl.Int64})
mnitb_current = pl.read_csv("MNITB_CURRENT.txt", schema={'ACCTNO': pl.Int64, 'CUSTCODE': pl.Int64})

# Load DCID
dcid = pl.read_csv(f"DCID{REPTMON}{REPTDAY}.txt", 
                   schema={'TICKETNO': pl.Utf8, 'CUSTCODE': pl.Int64})

# -------------------------------------------------------------------
# Step 3: DPST dataset
# -------------------------------------------------------------------
dpst = dpfl.with_columns([
    pl.col("ACCINT").cast(pl.Float64)
])

# Merge with DCID
dpst = dpst.join(dcid, on="TICKETNO", how="left")

# -------------------------------------------------------------------
# Step 4: EQC / EQI split
# -------------------------------------------------------------------
eq = eqfl.with_columns([
    pl.col("ACCINTRM").abs(),
    pl.col("ACCINTAMT").abs(),
    pl.col("TOTINTAMT").abs(),
    pl.col("PREMPAID").abs(),
    pl.col("PREMREC").abs()
])

# Filter by date range
eq = eq.filter((pl.col("STARTDT") <= str(today)) & (pl.col("MATDT") >= str(today)))

eqc = eq.filter(pl.col("TYPE") == "C")
eqi = eq.filter(pl.col("TYPE") != "C")

# Keep only necessary columns for EQC
eqc = eqc.select(['TICKETNO', 'CUSTICKETNO', 'BRANCH', 'INVCURR', 'ALTCURR',
                  'INVAMT', 'ALTAMT', 'TENOR', 'STATUSIND', 'DCIRT', 'STARTDT',
                  'MATDT', 'PREMPAID', 'TYPE'])

# Keep only necessary columns for EQI
eqi = eqi.select(['TICKETNO', 'CUSTNAME', 'CUSTRES', 'CUSTLOC', 'FISSCODE',
                  'CUSTICKETNO', 'BRANCH', 'INVCURR', 'ALTCURR', 'EQCUSTYP',
                  'INVAMT', 'ALTAMT', 'TENOR', 'STATUSIND', 'STARTDT',
                  'MATDT', 'PREMREC', 'TYPE'])

# -------------------------------------------------------------------
# Step 5: Customer leg (EQC join DPST, CRA, DEPO)
# -------------------------------------------------------------------
eqdci = dpst.join(eqc, on="TICKETNO", how="inner")
eqdci = eqdci.filter(pl.col("CUSTCODE") >= 80)

# CRA processing
dp_cra = cra.filter(pl.col("INV_STATUS").is_in(["ACT", "CEP", "CEU", "CCU", "CMU"]))
dp_cra = dp_cra.with_columns([
    pl.lit("Outstanding").alias("STATUSIND"),
    pl.lit("MYR").alias("INVCURR"),
    pl.lit(0.0).alias("PREMPAID"),
    pl.lit(0.0).alias("ACCINT")  # ACCINT already exists but we keep it
])

# Create DEPO dataset
depo = pl.concat([mnitb_saving, mnitb_current])
depo = depo.rename({"ACCTNO": "INVCURAC"})

# Join CRA with DEPO
dp_cra = dp_cra.join(depo, on="INVCURAC", how="inner")
dp_cra = dp_cra.filter(pl.col("CUSTCODE") >= 80)

# Combine EQDCI with CRA
eqdci = pl.concat([eqdci, dp_cra])

# FX enrichment
eqrt = eqrt.rename({"CURRENCY": "INVCURR", "SPOTRATE": "SPOTRT"})
eqdci = eqdci.join(eqrt.select(['INVCURR', 'SPOTRT']), on="INVCURR", how="left")

# Round based on currency
eqdci = eqdci.with_columns([
    pl.when(pl.col("INVCURR") == "JPY")
    .then(pl.col("ACCINT").round(0))
    .otherwise(pl.col("ACCINT").round(2))
    .alias("ACCINTX"),
    pl.when(pl.col("INVCURR") == "JPY")
    .then(pl.col("PREMPAID").round(0))
    .otherwise(pl.col("PREMPAID").round(2))
    .alias("PREMPAI")
])

eqdci = eqdci.with_columns([
    (pl.col("ACCINTX") * pl.col("SPOTRT")).alias("ACCINTRM"),
    (pl.col("PREMPAI") * pl.col("SPOTRT")).alias("PREMPAIDRM")
])

# Split into MYR and FCY
cusmyr = eqdci.filter(pl.col("INVCURR") == "MYR")
cusfcy = eqdci.filter(pl.col("INVCURR") != "MYR")

# Write text output for customer MYR
with open("DCITXT.txt", "w") as f:
    f.write("PUBLIC BANK BERHAD\n")
    f.write(f"DAILY EXTRACTION OF DCI/CRA CUSTOMER FOR MYR AS AT {RDATE}\n")
    # Write header
    cols = ['CUSTICKETNO', 'TICKETNO', 'CUSTNAME', 'CUSTCODE', 'BRANCH',
            'INVCURAC', 'ALTCURAC', 'INVCURR', 'ALTCURR', 'INVAMT', 'ALTAMT',
            'TENOR', 'SPOTRT', 'DCIRT', 'STATUSIND', 'STARTDT', 'MATDT',
            'ACCINT', 'ACCINTRM', 'PREMPAID', 'PREMPAIDRM']
    f.write(','.join(cols) + '\n')
    
    # Write data (simplified)
    for row in cusmyr.iter_rows(named=True):
        f.write(','.join([str(row.get(c, '')) for c in cols]) + '\n')

# Write text output for customer FCY
with open("DCITXT.txt", "a") as f:
    f.write("\nPUBLIC BANK BERHAD\n")
    f.write(f"DAILY EXTRACTION OF DCI/CRA CUSTOMER FOR FCY AS AT {RDATE}\n")
    for row in cusfcy.iter_rows(named=True):
        f.write(','.join([str(row.get(c, '')) for c in cols]) + '\n')

# -------------------------------------------------------------------
# Step 6: Interbank leg
# -------------------------------------------------------------------
eqdci_ib = eqi.filter(pl.col("FISSCODE") >= "80")
eqdci_ib = eqdci_ib.join(eqrt.select(['INVCURR', 'SPOTRT']), on="INVCURR", how="left")

eqdci_ib = eqdci_ib.with_columns([
    pl.when(pl.col("INVCURR") == "JPY")
    .then(pl.col("PREMREC").round(0))
    .otherwise(pl.col("PREMREC").round(2))
    .alias("PREMREX")
])

eqdci_ib = eqdci_ib.with_columns([
    (pl.col("PREMREX") * pl.col("SPOTRT")).alias("PREMRECRM")
])

# Split interbank
ibnmyr = eqdci_ib.filter(pl.col("INVCURR") == "MYR")
ibnfcy = eqdci_ib.filter(pl.col("INVCURR") != "MYR")

# Write text output for interbank MYR
with open("DCITXT.txt", "a") as f:
    f.write("\nPUBLIC BANK BERHAD\n")
    f.write(f"DAILY EXTRACTION OF DCI INTERBANK FOR MYR AS AT {RDATE}\n")
    cols = ['CUSTICKETNO', 'TICKETNO', 'CUSTNAME', 'CUSTRES', 'CUSTLOC',
            'FISSCODE', 'EQCUSTYP', 'BRANCH', 'INVCURR', 'ALTCURR',
            'INVAMT', 'ALTAMT', 'TENOR', 'SPOTRT', 'STATUSIND',
            'STARTDT', 'MATDT', 'PREMREC', 'PREMRECRM']
    f.write(','.join(cols) + '\n')
    for row in ibnmyr.iter_rows(named=True):
        f.write(','.join([str(row.get(c, '')) for c in cols]) + '\n')

# Write text output for interbank FCY
with open("DCITXT.txt", "a") as f:
    f.write("\nPUBLIC BANK BERHAD\n")
    f.write(f"DAILY EXTRACTION OF DCI INTERBANK FOR FCY AS AT {RDATE}\n")
    for row in ibnfcy.iter_rows(named=True):
        f.write(','.join([str(row.get(c, '')) for c in cols]) + '\n')

# -------------------------------------------------------------------
# Step 7: Build DCI
# -------------------------------------------------------------------
dcimyr = pl.concat([cusmyr, ibnmyr])

# Add PREMIUM column
dcimyr = dcimyr.with_columns([
    pl.when(pl.col("TYPE") == "C")
    .then(pl.col("PREMPAID"))
    .otherwise(pl.col("PREMREC"))
    .alias("PREMIUM"),
    pl.lit(today).alias("REPTDATS")
])

# Derive ELDAY (matches SAS logic exactly)
def calc_elday(d):
    dd = d.day
    mm = d.month
    yy = d.year
    
    # Base mapping
    if dd in (1, 9, 16, 23):
        elday = 'DAYA'
    elif dd in (2, 10, 17, 24):
        elday = 'DAYB'
    elif dd in (3, 11, 18, 25):
        elday = 'DAYC'
    elif dd in (4, 12, 19, 26):
        elday = 'DAYD'
    elif dd in (5, 13, 20, 27):
        elday = 'DAYE'
    elif dd in (6, 14, 21, 28):
        elday = 'DAYF'
    elif dd in (7, 29):
        elday = 'DAYG'
    elif dd == 30:
        elday = 'DAYH'
    elif dd in (8, 15, 22, 31):
        elday = 'DAYI'
    else:
        elday = 'DAYX'
    
    # Month adjustments (SAS logic)
    if mm in (4, 6, 9, 11) and dd == 30:
        elday = 'DAYI'
    
    # February adjustments
    if mm == 2:
        if dd == 28:
            elday = 'DAYI'
            if yy % 4 == 0:  # Leap year
                elday = 'DAYF'
        if dd == 29 and yy % 4 == 0:
            elday = 'DAYI'
    
    return elday

dcimyr = dcimyr.with_columns([
    pl.col("REPTDATS").map_elements(calc_elday).alias("ELDAY")
])

# Generate BNM records
records = []
for row in dcimyr.iter_rows(named=True):
    accintrm = row.get("ACCINTRM", 0)
    if accintrm not in (None, 0):
        records.append({
            "BNMCODE": "4911095000000Y",
            "ELDAY": row["ELDAY"],
            "REPTDATS": row["REPTDATS"],
            "AMOUNT": accintrm
        })
    
    premium = row.get("PREMIUM", 0)
    if premium not in (None, 0):
        records.append({
            "BNMCODE": "4929996000000Y",
            "ELDAY": row["ELDAY"],
            "REPTDATS": row["REPTDATS"],
            "AMOUNT": premium
        })

dci_final = pl.DataFrame(records)

# Aggregate
dci_final = dci_final.group_by(["BNMCODE", "ELDAY", "REPTDATS"]).agg(
    pl.sum("AMOUNT").alias("AMOUNT")
)

# -------------------------------------------------------------------
# Step 8: Write outputs
# -------------------------------------------------------------------
# Write Parquet output
out_file = f"DCI_{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
dci_final.write_parquet(out_file)
print(f"Final DCI Parquet written: {out_file}")

# Write SAS7bdat output
try:
    # Convert to pandas for pyreadstat
    dci_pd = dci_final.to_pandas()
    pyreadstat.write_sas7bdat(dci_pd, f"BNMK_DCI{REPTMON}{WK}.sas7bdat")
    print(f"SAS dataset written: BNMK_DCI{REPTMON}{WK}.sas7bdat")
except Exception as e:
    print(f"Could not write SAS dataset: {e}")

# Write CSV output
dci_final.write_csv(f"DCI_{REPTYEAR}{REPTMON}{REPTDAY}.csv")
print(f"CSV output written: DCI_{REPTYEAR}{REPTMON}{REPTDAY}.csv")

# -------------------------------------------------------------------
# Step 9: Register in DuckDB for analytics
# -------------------------------------------------------------------
duckdb.sql("INSTALL parquet; LOAD parquet;")
duckdb.sql(f"CREATE TABLE dci AS SELECT * FROM read_parquet('{out_file}')")
result = duckdb.sql("SELECT COUNT(*) FROM dci").fetchall()
print(f"DuckDB table created. Rowcount: {result[0][0]}")

print("EIBDCITX completed successfully!")
