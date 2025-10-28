# credit_reconciliation_check.py
import duckdb
import polars as pl
from pathlib import Path

# Read reporting date and set parameters - MISSING: DATA REPTDATE; SET LOAN.REPTDATE;
reptdate_df = duckdb.connect().execute("SELECT * FROM read_parquet('LOAN/REPTDATE.parquet')").pl()
reptdate = reptdate_df["REPTDATE"][0]

day = reptdate.day
if day == 8:
    nowk = '1'
elif day == 15:
    nowk = '2'
elif day == 22:
    nowk = '3'
else:
    nowk = '4'

reptyear = reptdate.strftime("%y")
reptmon = f"{reptdate.month:02d}"

# Process credit data - MISSING: DATA CRIS; INFILE CRED;
cred_schema = {
    "FICODE": (0, 9), "APCODE": (9, 3), "ACCTNO": (12, 10), "NOTENO": (42, 10),
    "OUTSTAND": (80, 16), "ACCTSTAT": (119, 1), "COSTCTR": (164, 4), 
    "FACILITY": (181, 5), "CURBALCR": (320, 17)
}

cris_df = pl.read_csv("CRED", has_header=False, new_columns=["raw"])
for col, (start, length) in cred_schema.items():
    cris_df = cris_df.with_columns(
        pl.col("raw").str.slice(start, length).str.strip_chars().alias(col)
    )

cris_df = cris_df.drop("raw").with_columns([
    pl.col("FICODE").cast(pl.Int64),
    pl.col("APCODE").cast(pl.Int64),
    pl.col("ACCTNO").cast(pl.Int64),
    pl.col("NOTENO").cast(pl.Int64),
    pl.col("OUTSTAND").cast(pl.Float64),
    pl.col("COSTCTR").cast(pl.Int64),
    pl.col("FACILITY").cast(pl.Int64),
    pl.col("CURBALCR").cast(pl.Float64)
]).filter(
    ~pl.col("ACCTSTAT").is_in(['S','W','F','K']) & (pl.col("COSTCTR") != 901)
).sort(["ACCTNO", "NOTENO"])

# MISSING: PROC SORT DATA=CRIS; BY ACCTNO NOTENO; RUN; (already done above)

# Read loan data - MISSING: PROC SORT DATA=LN.LOAN&REPTMON&NOWK OUT=LN;
loan_df = duckdb.connect().execute(f"SELECT * FROM read_parquet('LN/LOAN{reptmon}{nowk}.parquet')").pl()
loan_df = loan_df.sort(["ACCTNO", "NOTENO"])

# Merge and reconcile data - MISSING: DATA CRED LNCRED ODCRED;
merged_df = loan_df.join(cris_df, on=["ACCTNO", "NOTENO"], how="inner", suffix="_cred")
merged_df = merged_df.with_columns([
    (pl.col("BALANCE") - pl.col("OUTSTAND")).alias("DIFF")
]).filter(
    (pl.col("DIFF") <= -1) | (pl.col("DIFF") >= 1)
)

# Split into different account types - MISSING: OUTPUT CRED; IF...THEN OUTPUT ODCRED; ELSE OUTPUT LNCRED;
cred_final = merged_df
lncred_df = merged_df.filter(~pl.col("ACCTNO").is_between(3000000000, 3999999999))
odcred_df = merged_df.filter(pl.col("ACCTNO").is_between(3000000000, 3999999999))

# Create output directories
Path("credit_reconciliation_reports").mkdir(exist_ok=True)

# Output results
cred_final.write_parquet(f"credit_reconciliation_reports/all_discrepancies_{reptmon}{nowk}{reptyear}.parquet")
cred_final.write_csv(f"credit_reconciliation_reports/all_discrepancies_{reptmon}{nowk}{reptyear}.csv")

lncred_df.write_parquet(f"credit_reconciliation_reports/loan_discrepancies_{reptmon}{nowk}{reptyear}.parquet")
lncred_df.write_csv(f"credit_reconciliation_reports/loan_discrepancies_{reptmon}{nowk}{reptyear}.csv")

odcred_df.write_parquet(f"credit_reconciliation_reports/overdraft_discrepancies_{reptmon}{nowk}{reptyear}.parquet")
odcred_df.write_csv(f"credit_reconciliation_reports/overdraft_discrepancies_{reptmon}{nowk}{reptyear}.csv")

# MISSING: PROC PRINT DATA=LNCRED(OBS=100); TITLE 'LOANS';
print("="*80)
print("LOAN ACCOUNT DISCREPANCIES (First 100 Records)")
print("="*80)
loan_sample = lncred_df.head(100).select(["ACCTNO", "NOTENO", "BALANCE", "OUTSTAND", "DIFF", "FACILITY"])
print(loan_sample)

# MISSING: PROC PRINT DATA=ODCRED(OBS=100); TITLE 'OVERDRAFT';
print("\n" + "="*80)
print("OVERDRAFT ACCOUNT DISCREPANCIES (First 100 Records)")
print("="*80)
od_sample = odcred_df.head(100).select(["ACCTNO", "NOTENO", "BALANCE", "OUTSTAND", "DIFF", "FACILITY"])
print(od_sample)

# MISSING: %MACRO PROCESS; %LET DSET = %SYSFUNC(OPEN(CRED)); etc.
discrepancy_count = len(cred_final)
print(f"\nTOTAL DISCREPANT ACCOUNTS: {discrepancy_count}")

# MISSING: %IF &NOBS >= 10 %THEN %DO; DATA A; ABORT 77; RUN; %END;
if discrepancy_count >= 10:
    print(f"ERROR: {discrepancy_count} ACCOUNTS ARE NOT TALLY")
    print("ABORTING WITH EXIT CODE 77")
    exit(77)
else:
    print(f"SUCCESS: Reconciliation within acceptable limits ({discrepancy_count} discrepancies)")
