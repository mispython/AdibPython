import polars as pl
from datetime import timedelta
from pathlib import Path

# ==================== SETUP ====================
BASE_PATH = Path("/path/to/data")
PATHS = {k: BASE_PATH / k for k in ['dp', 'eqwh', 'bnmk', 'cisieq', 'cisdp', 'cisdp2', 'output']}

# ==================== DATE SETUP ====================
reptdate_val = pl.read_parquet(PATHS['dp'] / "REPTDATE.parquet")[0, "REPTDATE"]
REPTMON, REPTYEAR = f"{reptdate_val.month:02d}", str(reptdate_val.year)[-2:]
REPTMMMYY = reptdate_val.strftime("%b%y").upper()
print(f"Report Date: {reptdate_val.strftime('%d-%b-%Y').upper()}")

# ==================== MAPPINGS ====================
SME_CODES = ['41','42','43','44','46','47','48','49','51','52','53','54','66','67','68','69']
DNBFI_CODES = ['04','05','06','30','31','32','33','34','35','36','37','38','39','40','45']
FX_CODES = ['87','88','89']
VALID_CODES = SME_CODES + DNBFI_CODES + FX_CODES

CUST_GROUP_MAP = {**{c: 'SMALL MEDIUM ENTERPRISE (SMES)' for c in SME_CODES},
                  **{c: 'DOMESTIC (DNBFI)' for c in DNBFI_CODES + ['4','04','5','05','6','06']},
                  **{c: 'NON-RESIDENT/FOREIGN ENTITIES' for c in FX_CODES}}

CUST_TYPE_MAP = {
    **{c: 'BUMIPUTRA CONTROLLED SME' for c in ['41','42','43','66']},
    **{c: 'NON-BUMIPUTRA CONTROLLED SME' for c in ['44','46','47','67']},
    **{c: 'NON-RESIDENT CONTROLLED SME' for c in ['48','49','51','68']},
    **{c: 'GOVERNMENT CONTROLLED SME' for c in ['52','53','54','69']},
    '04': 'SUBSIDIARY STOCKBROKING COS.', '05': 'ASSOCIATE STOCKBROKING COS.',
    '06': 'OTHER STOCKBROKING COS.', '30': 'DOMESTIC OTHER NBFI',
    '31': 'SAVING INSTITUTIONS', '32': 'CREDIT CARD COMPANIES',
    '33': 'DEVELOPMENT FINANCE INSTITUTION', '34': 'BUILDING SOCIETIES',
    '35': 'CO-OPERATIVE SOCIESTIES', '36': 'STOCKBROKING COMPANIES',
    '37': 'COMMODITY BROKERS', '38': 'CREDIT AND LEASING COMPANIES',
    '39': 'UNIT TRUST COMPANIES', '40': 'INSURANCE COS AND INSURANCE RELATED ENT.',
    '45': 'REHABILITATION INSTITUTIONS',
    **{c: 'FOREIGN BUSINESS ENTERPRISES' for c in FX_CODES}
}

# ==================== HELPER FUNCTIONS ====================
def process_islamic_account(file, filters, prodcd=None, custfiss_expr=None, extra_cols=None):
    """Generic Islamic account processing"""
    cols = ["ACCTNO", "CUSTFISS", "DNBFISME", "CURBAL"] + (extra_cols or [])
    df = pl.read_parquet(PATHS['dp'] / file).filter(filters)
    
    if custfiss_expr is None:
        custfiss_expr = pl.col("CUSTCODE").cast(pl.Utf8).str.zfill(2)
    
    df = df.with_columns([custfiss_expr.alias("CUSTFISS")])
    if prodcd:
        df = df.with_columns([pl.lit(prodcd).alias("PRODCD")])
    
    return df.filter(
        (pl.col("CUSTFISS").is_in(VALID_CODES)) | 
        (pl.col("DNBFISME").is_in(['1','2','3']) if "DNBFISME" in df.columns else pl.lit(False))
    ).select([c for c in cols if c in df.columns or c == "PRODCD"])

def add_mappings(df):
    """Add customer group and type mappings"""
    return df.with_columns([
        df["CUSTFISS"].map_dict(CUST_GROUP_MAP).alias("CUSTGROUP"),
        df["CUSTFISS"].map_dict(CUST_TYPE_MAP).alias("CUSTTYPE"),
        pl.col("CURBAL").alias("AMOUNT")
    ])

def write_islamic_report(filename, lines):
    """Write report with ASA control"""
    with open(PATHS['output'] / filename, 'w') as f:
        for i, line in enumerate(lines):
            prefix = "\x0c" if i == 0 or i % 60 == 0 else " "
            f.write(prefix + line + '\n')

# ==================== MNI DATA ====================
print("Processing Islamic MNI data...")

# Current Accounts
ca_df = process_islamic_account(
    "CURRENT.parquet",
    (~pl.col("OPENIND").is_in(["B","C","P"])) & 
    (((pl.col("PRODUCT") >= 400) & (pl.col("PRODUCT") <= 444)) | pl.col("PRODUCT").is_in([63,163,413])) &
    (pl.col("CURBAL") > 0) & (~pl.col("PRODUCT").is_in([63,163,413])),
    "ICA",
    pl.when(pl.col("PRODUCT")==104).then(pl.lit("02"))
      .when(pl.col("PRODUCT")==105).then(pl.lit("81"))
      .otherwise(pl.col("CUSTCODE").cast(pl.Utf8).str.zfill(2))
)

# Savings Accounts (filtered by product '42120')
sa_df = process_islamic_account(
    "SAVING.parquet",
    (~pl.col("OPENIND").is_in(["B","C","P"])) & (pl.col("CURBAL") > 0),
    "ISA"
).filter(pl.col("PRODCD") == "42120")

# UMA Accounts
uma_df = process_islamic_account(
    "UMA.parquet",
    (pl.col("OPENIND").is_in(["D","O"])) & (pl.col("CURBAL") > 0),
    custfiss_expr=pl.when(pl.col("PRODUCT")==297)
                     .then(pl.col("CUSTCODE").cast(pl.Utf8).str.zfill(2))
                     .otherwise(pl.col("CUSTCODE").cast(pl.Utf8).str.zfill(2))
)

# FD Accounts
fd_df = process_islamic_account(
    "FD.parquet",
    pl.col("OPENIND").is_in(["D","O"]),
    extra_cols=["PRODUCT"]
).with_columns([pl.col("PRODUCT").cast(pl.Utf8).alias("PRODCD")])

# Combine MNI
mni_df = add_mappings(pl.concat([ca_df, sa_df, uma_df, fd_df]))

# ==================== EQ DATA ====================
print("Processing Islamic EQ data...")
eq_list = []

# IUTMS file
eq_file = PATHS['eqwh'] / f"IUTMS{REPTMON}.4{REPTYEAR}.parquet"
if eq_file.exists():
    eq_list.append(
        pl.read_parquet(eq_file).drop("CUSTNAME").filter(
            pl.col("SECTYPE").is_in(['IFD','ILD','ISD','IZD','IDC','IDP','IZP']) &
            pl.col("PORTREF").is_in(['PFD','PLD','PSD','PZD','PDC']) &
            (pl.col("CUSTFISS").is_in(VALID_CODES) | 
             ((pl.col("CUSTFISS").cast(pl.Int32) >= 30) & (pl.col("CUSTFISS").cast(pl.Int32) <= 45)))
        ).with_columns([pl.col("AMTOWNED").alias("AMOUNT"), pl.col("CUSTEQNO").alias("ACCTNO")])
    )

# K1TBL file (Islamic deals)
k1_file = PATHS['bnmk'] / f"K1TBL{REPTMON}.4.parquet"
if k1_file.exists():
    eq_list.append(
        pl.read_parquet(k1_file).filter(
            ((pl.col("GWCCY")=="MYR") & (pl.col("GWMVT")=="P") & (pl.col("GWMVTS")=="M") & 
             pl.col("GWDLP").is_in(['BCS','BCT','BCW','BQD'])) |  # Islamic deals
            ((pl.col("GWDLP").str.slice(1,2).is_in(['MI','MT'])) & (pl.col("GWCCY")=="MYR") & 
             (pl.col("GWMVT")=="P") & (pl.col("GWMVTS")=="M"))
        ).with_columns([
            pl.col("GWC2R").alias("CUSTFISS"),
            pl.col("GWAN").cast(pl.Int64).alias("ACCTNO"),
            pl.col("GWBALC").alias("AMOUNT")
        ]).filter(
            pl.col("CUSTFISS").is_in(VALID_CODES) | 
            ((pl.col("CUSTFISS").cast(pl.Int32) >= 30) & (pl.col("CUSTFISS").cast(pl.Int32) <= 45))
        )
    )

eq_combined = (pl.concat(eq_list).with_columns([
    pl.col("CUSTFISS").map_dict(CUST_GROUP_MAP).alias("CUSTGROUP"),
    pl.col("CUSTFISS").map_dict(CUST_TYPE_MAP).alias("CUSTTYPE")
]).select(["ACCTNO", "CUSTFISS", "AMOUNT", "CUSTGROUP", "CUSTTYPE"])
) if eq_list else pl.DataFrame()

# ==================== MERGE WITH CUSTOMER NAMES ====================
print("Merging customer names...")
cisdp = pl.concat([
    pl.read_parquet(PATHS[k] / "DEPOSIT.parquet").select(["ACCTNO","CUSTNAME"]) 
    for k in ['cisdp','cisdp2']
]).unique(subset=["ACCTNO"])
cisieq = pl.read_parquet(PATHS['cisieq'] / "DEPOSIT.parquet").select(["ACCTNO","CUSTNAME"])

mni_final = mni_df.join(cisdp, on="ACCTNO", how="left") if len(mni_df) > 0 else mni_df
eq_final = eq_combined.join(cisieq, on="ACCTNO", how="left") if len(eq_combined) > 0 else eq_combined

# ==================== SUMMARIZE ====================
print("Summarizing Islamic data...")
all_data = pl.concat([
    df.select(["CUSTGROUP","CUSTTYPE","AMOUNT"]) 
    for df in [mni_final, eq_final] if len(df) > 0
]).filter(pl.col("CUSTGROUP").is_not_null())

summary = all_data.group_by(["CUSTGROUP","CUSTTYPE"]).agg([pl.col("AMOUNT").sum()])
grand_total = all_data.select(pl.col("AMOUNT").sum().alias("AMOUNT")).with_columns([
    pl.lit("GRAND TOTAL").alias("CUSTTYPE")
])

# ==================== GENERATE REPORTS ====================
print("Generating Islamic reports...")

for group, prefix, title in [
    ('SMALL MEDIUM ENTERPRISE (SMES)', 'SME', 'DEPOSITS ACCEPTED FROM SMALL MEDIUM ENTERPRISES (SME)'),
    ('DOMESTIC (DNBFI)', 'DNBFI', 'DOMESTIC (DNBFI)'),
    ('NON-RESIDENT/FOREIGN ENTITIES', 'FX', 'NON-RESIDENT/FOREIGN ENTITIES')
]:
    df = summary.filter(pl.col("CUSTGROUP") == group)
    if len(df) > 0:
        total_row = df.select(pl.col("AMOUNT").sum().alias("AMOUNT")).with_columns([
            pl.lit("TOTAL").alias("CUSTTYPE")
        ])
        df = pl.concat([df, total_row]).sort("CUSTTYPE")
        
        lines = [
            " "*40 + "PUBLIC ISLAMIC BANK BERHAD",
            " "*20 + title,
            " "*40 + f"@ {REPTMMMYY}",
            "", group if prefix == 'SME' else "", "="*80 if prefix == 'SME' else "-"*80,
            f"{'CUSTOMER':<40} {'TOTAL OS MYR':>20}"
        ]
        lines.extend([f"{row['CUSTTYPE']:<40} {row['AMOUNT']:>20,.2f}" 
                     for row in df.iter_rows(named=True)])
        
        write_islamic_report(f"ISLAMIC_{prefix}_REPORT_{REPTMMMYY}.txt", lines)
        print(f"Islamic {prefix} Total: {total_row['AMOUNT'][0]:,.2f}")

# Grand Total
write_islamic_report(f"ISLAMIC_GRAND_TOTAL_{REPTMMMYY}.txt", 
                     [f"{'GRAND TOTAL':<40} {grand_total[0,'AMOUNT']:>20,.2f}"])
print(f"Islamic Grand Total: {grand_total[0,'AMOUNT']:,.2f}")

# ==================== DETAILED LISTS ====================
print("Generating Islamic detailed lists...")

# EQ List
if len(eq_final) > 0:
    lines = [
        " "*40 + "PUBLIC ISLAMIC BANK BERHAD",
        " "*40 + "EQ CUSTOMER LIST",
        " "*40 + f"@ {REPTMMMYY}",
        "", "="*100,
        f"{'ACCTNO':<12} {'CUSTNAME':<40} {'CUSTTYPE':<30} {'AMOUNT':>15}"
    ]
    lines.extend([
        f"{row.get('ACCTNO',''):<12} {row.get('CUSTNAME',''):<40} "
        f"{row.get('CUSTTYPE',''):<30} {row.get('AMOUNT',0):>15,.2f}"
        for row in eq_final.sort("ACCTNO").iter_rows(named=True)
    ])
    write_islamic_report(f"ISLAMIC_EQ_LIST_{REPTMMMYY}.txt", lines)

# MNI List
if len(mni_final) > 0:
    lines = [
        " "*40 + "PUBLIC ISLAMIC BANK BERHAD",
        " "*40 + "MNI CUSTOMER LIST",
        " "*40 + f"@ {REPTMMMYY}",
        "", "="*120,
        f"{'ACCTNO':<12} {'CUSTNAME':<40} {'CUSTFISS':<8} {'DNBFISME':<8} "
        f"{'PRODCD':<10} {'CUSTTYPE':<30} {'AMOUNT':>15}"
    ]
    lines.extend([
        f"{row.get('ACCTNO',''):<12} {row.get('CUSTNAME',''):<40} "
        f"{row.get('CUSTFISS',''):<8} {row.get('DNBFISME',''):<8} "
        f"{row.get('PRODCD',''):<10} {row.get('CUSTTYPE',''):<30} "
        f"{row.get('AMOUNT',0):>15,.2f}"
        for row in mni_final.sort("ACCTNO").iter_rows(named=True)
    ])
    write_islamic_report(f"ISLAMIC_MNI_LIST_{REPTMMMYY}.txt", lines)

print("Islamic processing complete!")
