import polars as pl
import duckdb
from datetime import datetime
from pathlib import Path

# ==================== SETUP ====================
BASE_PATH = Path("/path/to/data")
CIS_PATH = BASE_PATH / "cis"
DPTRBL_PATH = BASE_PATH / "dptrbl"
MNICRM_PATH = BASE_PATH / "mnicrm"
OD_PATH = BASE_PATH / "od"
SIGNA_PATH = BASE_PATH / "signa"
MISMTD_PATH = BASE_PATH / "mismtd"
DRCRCA_PATH = BASE_PATH / "drcrca"
CURRENT_PATH = BASE_PATH / "current"
OUTPUT_PATH = BASE_PATH / "output"

conn = duckdb.connect()

# ==================== CIS DATA PROCESSING ====================
print("Processing CIS data...")
cis_df = pl.read_parquet(CIS_PATH / "DEPOSIT.parquet").filter(
    pl.col("SECCUST") == "901"
).with_columns([
    pl.col("CITIZEN").alias("COUNTRY"),
    pl.col("BIRTHDAT").str.slice(0, 2).cast(pl.Int32).alias("BDD"),
    pl.col("BIRTHDAT").str.slice(2, 2).cast(pl.Int32).alias("BMM"),
    pl.col("BIRTHDAT").str.slice(4, 4).cast(pl.Int32).alias("BYY"),
    pl.col("CUSTNAM1").alias("NAME")
]).with_columns([
    pl.date(pl.col("BYY"), pl.col("BMM"), pl.col("BDD")).alias("DOBCIS")
]).select(["ACCTNO", "COUNTRY", "DOBCIS", "RACE", "NAME"])

cis_df = cis_df.unique(subset=["ACCTNO"])

# ==================== REPTDATE PROCESSING ====================
print("Processing report dates...")
reptdate_df = pl.read_parquet(DPTRBL_PATH / "REPTDATE.parquet")
reptdate_val = reptdate_df[0, "REPTDATE"]

day_val = reptdate_val.day
month_val = reptdate_val.month

# Determine week
if 1 <= day_val <= 8:
    WK = "1"
elif 9 <= day_val <= 15:
    WK = "2"
elif 16 <= day_val <= 22:
    WK = "3"
else:
    WK = "4"

# Determine month for CRM data
if WK != "4":
    MM = month_val - 1
else:
    MM = month_val

if MM == 0:
    MM = 12

NOWK = WK
REPTYEAR = str(reptdate_val.year)[-2:]
REPTMON = f"{month_val:02d}"
REPTMON1 = f"{MM:02d}"
REPTDAY = f"{day_val:02d}"
RDATE = reptdate_val.strftime("%d%m%Y")

# Save report date
report_date_df = pl.DataFrame({
    "REPTDATE": [reptdate_val],
    "WK": [WK],
    "MM": [MM]
})
report_date_df.write_parquet(CURRENT_PATH / "REPTDATE.parquet")

# ==================== COLUMN DEFINITIONS ====================
CA_COLS = [
    "ACCTNO", "BRANCH", "PRODUCT", "DEPTYPE", "ORGTYPE", "ORGCODE",
    "OPENDT", "OPENIND", "SECTOR", "STATE", "MTDAVBAL", "DNBFISME",
    "RISKCODE", "CUSTCODE", "LEDGBAL", "INTPAYBL", "CURBAL", "APPRLIMT",
    "ODPLAN", "RATE1", "RATE2", "RATE3", "RATE4", "RATE5", "FLATRATE",
    "LIMIT1", "LIMIT2", "LIMIT3", "LIMIT4", "LIMIT5", "PURPOSE", "USER2",
    "USER3", "LASTTRAN", "INTPLAN", "INTPD", "USER2", "SECOND", "INACTIVE",
    "BONUTYPE", "SERVICE", "USER5", "ODINTACC", "PREVBRNO", "CUSTFISS",
    "CHQFLOAT", "CRRCODE", "FAACRR", "COSTCTR", "CENSUST", "FEEYTD",
    "INTYTD", "BONUSANO", "RISKRATE", "ODINTCHR", "CLOSEDT", "DNBFI_ORI",
    "DOBMNI", "MAXPROF", "ACCPROF", "INTRSTPD", "YTDAVAMT", "AVGAMT",
    "WRITE_DOWN_BAL", "CHGIND", "RETURNS_Y", "DATE_LST_DEP", "L_DEP",
    "MAILCODE", "RRIND", "RRCOMPLDT", "BILLERIND", "ASCORE_PERM",
    "ASCORE_LTST", "INTCYCODE", "DTLSTCUST", "AUTHORISE_LIMIT",
    "ACSTATUS4", "ACSTATUS5", "ACSTATUS6", "ACSTATUS7", "ACSTATUS8",
    "ACSTATUS9", "ACSTATUS10", "POST_IND", "REASON", "INSTRUCTIONS",
    "DSR", "REPAY_TYPE_CD", "MTD_REPAID_AMT", "INDUSTRIAL_SECTOR_CD",
    "OMNILOAN", "OMNITRDB", "ASCORE_COMM", "REOPENDT",
    "PB_ENTERPRISE_REG_DT", "PB_ENTERPRISE_TAG",
    "MTD_DISBURSED_AMT", "MTD_REPAY_TYPE10_AMT",
    "MTD_REPAY_TYPE20_AMT", "MTD_REPAY_TYPE30_AMT",
    "MODIFIED_FACILITY_IND", "STMT_CYCLE", "NXT_STMT_CYCLE_DT",
    "COV_OPT_OUT", "RR_EXP_DATE", "RR_EFF_DATE",
    "PROP_DEVELOP_FIN_IND", "INTPLAN_IBCA",
    "PB_ENTERPRISE_PACKAGE_CD"
]

# ==================== MONTHLY DATA PROCESSING ====================
print(f"Processing monthly data (Week: {NOWK}, Month: {REPTMON})...")

if NOWK != "4" and REPTMON == "01":
    # First week of January
    ca_df = pl.read_parquet(DPTRBL_PATH / "CURRENT.parquet").with_columns([
        pl.lit(0).alias("FEEYTD")
    ])
else:
    # Other weeks - merge with CRM data
    crm_file = MNICRM_PATH / f"CA{REPTMON1}.parquet"
    if crm_file.exists():
        crm_df = pl.read_parquet(crm_file).unique(subset=["ACCTNO"])
        crm_df = crm_df.drop("COSTCTR") if "COSTCTR" in crm_df.columns else crm_df
        
        current_df = pl.read_parquet(DPTRBL_PATH / "CURRENT.parquet")
        ca_df = current_df.join(crm_df, on="ACCTNO", how="left", suffix="_crm")
    else:
        ca_df = pl.read_parquet(DPTRBL_PATH / "CURRENT.parquet")

# ==================== GP3 DATA FOR WEEK 4 ====================
if NOWK == "4":
    print("Merging GP3 data for week 4...")
    gp3_file = OD_PATH / "GP3.parquet"
    if gp3_file.exists():
        gp3_df = pl.read_parquet(gp3_file)
        ca_df = ca_df.join(gp3_df, on="ACCTNO", how="left")

# ==================== OVERDFT DATA ====================
print("Processing overdraft data...")
overdft_df = pl.read_parquet(OD_PATH / "OVERDFT.parquet").select([
    "ACCTNO", "CRRINI", "CRRNOW", "ASCORE_PERM", "ASCORE_LTST", "ASCORE_COMM"
]).unique(subset=["ACCTNO"])

# ==================== MAIN CURRENT DATA PROCESSING ====================
print("Processing current accounts...")
ca_df = ca_df.drop(["FAACRR", "CRRCODE"])

curr_df = ca_df.join(
    overdft_df.rename({"CRRINI": "FAACRR", "CRRNOW": "CRRCODE"}),
    on="ACCTNO",
    how="left"
)

# Filter and transform
curr_df = curr_df.filter(
    pl.col("OPENIND").is_in(["B", "C", "P"]).not_() &
    ((pl.col("PRODUCT") < 400) | (pl.col("PRODUCT") > 411)) &
    (pl.col("PRODUCT") != 413) &
    ((pl.col("PRODUCT") < 420) | (pl.col("PRODUCT") > 434)) &
    (pl.col("PRODUCT") != 320)
)

# Add derived columns
curr_df = curr_df.with_columns([
    pl.when(pl.col("PRODUCT") == 104)
    .then(pl.lit("02"))
    .when(pl.col("PRODUCT") == 105)
    .then(pl.lit("81"))
    .otherwise(pl.col("CUSTCODE").cast(pl.Utf8)).alias("CUSTFISS"),
    
    pl.col("RISKCODE").cast(pl.Utf8).str.slice(0, 1).alias("RISKRATE"),
    
    # Parse dates
    pl.when((pl.col("BDATE") != 0) & pl.col("BDATE").is_not_null())
    .then(pl.col("BDATE").cast(pl.Utf8).str.slice(0, 8).str.strptime(pl.Date, "%m%d%Y"))
    .otherwise(pl.date(1900, 1, 1)).alias("DOBMNI"),
    
    pl.when((pl.col("OPENDT") != 0) & pl.col("OPENDT").is_not_null())
    .then(pl.col("OPENDT").cast(pl.Utf8).str.slice(0, 8).str.strptime(pl.Date, "%m%d%Y"))
    .otherwise(pl.date(1900, 1, 1)).alias("OPENDT_PARSED"),
    
    pl.when((pl.col("REOPENDT") != 0) & pl.col("REOPENDT").is_not_null())
    .then(pl.col("REOPENDT").cast(pl.Utf8).str.slice(0, 8).str.strptime(pl.Date, "%m%d%Y"))
    .otherwise(None).alias("REOPENDT_PARSED"),
    
    pl.when((pl.col("CLOSEDT") != 0) & pl.col("CLOSEDT").is_not_null())
    .then(pl.col("CLOSEDT").cast(pl.Utf8).str.slice(0, 8).str.strptime(pl.Date, "%m%d%Y"))
    .otherwise(None).alias("CLOSEDT_PARSED"),
    
    pl.when((pl.col("LASTTRAN") != 0) & pl.col("LASTTRAN").is_not_null())
    .then(pl.col("LASTTRAN").cast(pl.Utf8).str.slice(0, 6).str.strptime(pl.Date, "%m%d%y"))
    .otherwise(pl.date(1900, 1, 1)).alias("LASTTRAN_PARSED"),
    
    pl.when((pl.col("DTLSTCUST") != 0) & pl.col("DTLSTCUST").is_not_null())
    .then(pl.col("DTLSTCUST").cast(pl.Utf8).str.slice(0, 6).str.strptime(pl.Date, "%m%d%y"))
    .otherwise(None).alias("DTLSTCUST_PARSED"),
    
    pl.when(pl.col("RRCOMPLDT").is_not_null())
    .then(pl.col("RRCOMPLDT").cast(pl.Utf8).str.slice(0, 8).str.strptime(pl.Date, "%m%d%Y"))
    .otherwise(None).alias("RRCOMPLDT_PARSED"),
    
    pl.when((pl.col("RREXPIRYDT") != 0) & pl.col("RREXPIRYDT").is_not_null())
    .then(pl.col("RREXPIRYDT").cast(pl.Utf8).str.slice(0, 8).str.strptime(pl.Date, "%m%d%Y"))
    .otherwise(None).alias("RR_EXP_DATE"),
    
    pl.when((pl.col("RRMAINDT") != 0) & pl.col("RRMAINDT").is_not_null())
    .then(pl.col("RRMAINDT").cast(pl.Utf8).str.slice(0, 8).str.strptime(pl.Date, "%m%d%Y"))
    .otherwise(None).alias("RR_EFF_DATE"),
    
    # DNBFI logic
    pl.col("DNBFISME").alias("DNBFI_ORI"),
    
    pl.when(pl.col("PURPOSE") == "4")
    .then(pl.lit("1"))
    .otherwise(pl.col("PURPOSE")).alias("PURPOSE"),
    
    pl.when(~pl.col("CUSTFISS").is_in(["04", "05", "06", "30", "31", "32", "33", 
                                       "34", "35", "37", "38", "39", "40", "45"]))
    .then(pl.lit("0"))
    .otherwise(pl.col("DNBFISME")).alias("DNBFISME")
])

# Add ACSTATUS columns from STATCD
for i in range(4, 11):
    curr_df = curr_df.with_columns([
        pl.col("STATCD").cast(pl.Utf8).str.slice(i-1, 1).alias(f"ACSTATUS{i}")
    ])

# Rename PSREASON to REASON
if "PSREASON" in curr_df.columns:
    curr_df = curr_df.rename({"PSREASON": "REASON"})

# Select only required columns
available_cols = [col for col in CA_COLS if col in curr_df.columns]
curr_df = curr_df.select(available_cols)

# Sort by ACCTNO
curr_df = curr_df.sort("ACCTNO")

# ==================== MERGE WITH ADDITIONAL DATA ====================
print("Merging with additional data sources...")

# SMSACC data
saacc_df = pl.read_parquet(SIGNA_PATH / "SMSACC.parquet").sort("ACCTNO")

# MISMTD data
mismtd_file = MISMTD_PATH / f"CAVG{REPTMON}.parquet"
if mismtd_file.exists():
    mismtd_df = pl.read_parquet(mismtd_file)
    if "MTDAVFORBAL_MIS" in mismtd_df.columns:
        mismtd_df = mismtd_df.drop("MTDAVFORBAL_MIS")
else:
    mismtd_df = pl.DataFrame()

# Merge all data
final_df = curr_df.join(cis_df, on="ACCTNO", how="left")
final_df = final_df.join(saacc_df, on="ACCTNO", how="left")

if len(mismtd_df) > 0:
    final_df = final_df.join(mismtd_df, on="ACCTNO", how="left")

# Add ESIGNATURE default
if "ESIGNATURE" not in final_df.columns:
    final_df = final_df.with_columns(pl.lit("N").alias("ESIGNATURE"))
else:
    final_df = final_df.with_columns(
        pl.when((pl.col("ESIGNATURE") == "") | pl.col("ESIGNATURE").is_null())
        .then(pl.lit("N"))
        .otherwise(pl.col("ESIGNATURE"))
    )

# ==================== ACCUM DATA ====================
print("Merging ACCUM data...")
accum_df = pl.read_parquet(DRCRCA_PATH / "ACCUM.parquet").sort("ACCTNO")
final_df = final_df.join(accum_df, on="ACCTNO", how="left")

# ==================== SAVE FINAL OUTPUT ====================
output_file = CURRENT_PATH / f"CA{REPTMON}{NOWK}{REPTYEAR}.parquet"
final_df.write_parquet(output_file)
print(f"Output saved: {output_file}")

# ==================== GENERATE FILES FOR TRANSFER ====================
print("Generating transfer files...")

# For CPORT equivalent - create a compressed archive or flat file
transfile_path = OUTPUT_PATH / "SAP.PBB.CADATAWH.CAFTP"
with open(transfile_path, 'w') as f:
    # Write metadata header
    f.write(f"CA Data Export - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write(f"Report Period: {REPTMON}/{NOWK}/{REPTYEAR}\n")
    f.write("=" * 80 + "\n\n")
    
    # Write sample data (first 50 records)
    sample_df = final_df.head(50)
    for row in sample_df.iter_rows(named=True):
        f.write(str(row) + "\n")

# ==================== GENERATE SAMPLE REPORTS ====================
print("Generating sample reports...")

# Sample current data report
sample_file = OUTPUT_PATH / f"CA_SAMPLE_{REPTMON}{NOWK}{REPTYEAR}.txt"
with open(sample_file, 'w') as f:
    f.write(f"Sample Current Data - First 50 Records\n")
    f.write(f"Report Date: {RDATE}\n")
    f.write("=" * 100 + "\n")
    
    for row in final_df.head(50).iter_rows(named=True):
        f.write(str(row) + "\n")

# Report date info
date_file = OUTPUT_PATH / f"REPDATE_INFO_{REPTMON}{NOWK}{REPTYEAR}.txt"
with open(date_file, 'w') as f:
    f.write(f"Report Date Information\n")
    f.write("=" * 50 + "\n")
    f.write(f"Original Date: {reptdate_val}\n")
    f.write(f"Week: {WK}\n")
    f.write(f"Report Month: {REPTMON}\n")
    f.write(f"CRM Month: {REPTMON1}\n")
    f.write(f"Output File: CA{REPTMON}{NOWK}{REPTYEAR}.parquet\n")

print("Processing complete!")
print(f"Total records processed: {len(final_df)}")
print(f"Week: {WK}, Month: {REPTMON}, Year: {REPTYEAR}")

conn.close()
